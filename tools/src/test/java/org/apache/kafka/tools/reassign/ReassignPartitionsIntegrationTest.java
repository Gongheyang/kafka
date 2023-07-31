/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.tools.reassign;

import com.fasterxml.jackson.core.JsonProcessingException;
import kafka.server.IsrChangePropagationConfig;
import kafka.server.KafkaBroker;
import kafka.server.KafkaConfig;
import kafka.server.QuorumTestHarness;
import kafka.server.ZkAlterPartitionManager;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Some$;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.server.common.MetadataVersion.IBP_2_7_IV1;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.BROKER_LEVEL_THROTTLES;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.cancelAssignment;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.executeAssignment;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.verifyAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(300)
public class ReassignPartitionsIntegrationTest extends QuorumTestHarness {
    ReassignPartitionsTestCluster cluster;

    @AfterEach
    @Override
    public void tearDown() {
        Utils.closeQuietly(cluster, "ReassignPartitionsTestCluster");
        super.tearDown();
    }

    private final Map<Integer, Map<String, Long>> unthrottledBrokerConfigs = new HashMap<>(); {
        IntStream.range(0, 4).forEach(brokerId ->
            unthrottledBrokerConfigs.put(brokerId, BROKER_LEVEL_THROTTLES.stream()
                .collect(Collectors.toMap(throttle -> throttle, throttle -> -1L))));
    }

    @ParameterizedTest(name = "{displayName}.quorum={0}")
    @ValueSource(strings = {"zk", "kraft"})
    public void testReassignment(String quorum) throws Exception {
        ReassignPartitionsTestCluster cluster = new ReassignPartitionsTestCluster(Collections.emptyMap(), Collections.emptyMap());
        cluster.setup();
        executeAndVerifyReassignment();
    }

    @ParameterizedTest(name = "{displayName}.quorum={0}")
    @ValueSource(strings = "zk") // Note: KRaft requires AlterPartition
    public void testReassignmentWithAlterPartitionDisabled(String quorum) throws Exception {
        // Test reassignment when the IBP is on an older version which does not use
        // the `AlterPartition` API. In this case, the controller will register individual
        // watches for each reassigning partition so that the reassignment can be
        // completed as soon as the ISR is expanded.
        Map<String, String> configOverrides = Collections.singletonMap(KafkaConfig.InterBrokerProtocolVersionProp(), IBP_2_7_IV1.version());
        cluster = new ReassignPartitionsTestCluster(configOverrides, Collections.emptyMap());
        cluster.setup();
        executeAndVerifyReassignment();
    }

    @ParameterizedTest(name = "{displayName}.quorum={0}")
    @ValueSource(strings = "zk") // Note: KRaft requires AlterPartition
    public void testReassignmentCompletionDuringPartialUpgrade(String quorum) throws Exception {
        // Test reassignment during a partial upgrade when some brokers are relying on
        // `AlterPartition` and some rely on the old notification logic through Zookeeper.
        // In this test case, broker 0 starts up first on the latest IBP and is typically
        // elected as controller. The three remaining brokers start up on the older IBP.
        // We want to ensure that reassignment can still complete through the ISR change
        // notification path even though the controller expects `AlterPartition`.

        // Override change notification settings so that test is not delayed by ISR
        // change notification delay
        ZkAlterPartitionManager.DefaultIsrPropagationConfig_$eq(new IsrChangePropagationConfig(500, 100, 500));

        Map<String, String> oldIbpConfig = Collections.singletonMap(KafkaConfig.InterBrokerProtocolVersionProp(), IBP_2_7_IV1.version());
        Map<Integer, Map<String, String>> brokerConfigOverrides = new HashMap<>();
        brokerConfigOverrides.put(1, oldIbpConfig);
        brokerConfigOverrides.put(2, oldIbpConfig);
        brokerConfigOverrides.put(3, oldIbpConfig);

        cluster = new ReassignPartitionsTestCluster(Collections.emptyMap(), brokerConfigOverrides);
        cluster.setup();

        executeAndVerifyReassignment();
    }

    private void executeAndVerifyReassignment() throws ExecutionException, InterruptedException, JsonProcessingException {
        String assignment = "{\"version\":1,\"partitions\":" +
            "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}," +
            "{\"topic\":\"bar\",\"partition\":0,\"replicas\":[3,2,0],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
            "]}";

        TopicPartition foo0 = new TopicPartition("foo", 0);
        TopicPartition bar0 = new TopicPartition("bar", 0);

        // Check that the assignment has not yet been started yet.
        Map<TopicPartition, PartitionReassignmentState> initialAssignment = new HashMap<>();

        initialAssignment.put(foo0, new PartitionReassignmentState(Arrays.asList(0, 1, 2), Arrays.asList(0, 1, 3), true));
        initialAssignment.put(bar0, new PartitionReassignmentState(Arrays.asList(3, 2, 1), Arrays.asList(3, 2, 0), true));

        waitForVerifyAssignment(cluster.adminClient, assignment, false,
            new VerifyAssignmentResult(initialAssignment));

/*
        // Execute the assignment
        runExecuteAssignment(cluster.adminClient, false, assignment, -1L, -1L);
        assertEquals(unthrottledBrokerConfigs, describeBrokerLevelThrottles(unthrottledBrokerConfigs.keySet()));
        val finalAssignment = Map(
            foo0 -> PartitionReassignmentState(Seq(0, 1, 3), Seq(0, 1, 3), true),
            bar0 -> PartitionReassignmentState(Seq(3, 2, 0), Seq(3, 2, 0), true)
        )

        val verifyAssignmentResult = runVerifyAssignment(cluster.adminClient, assignment, false)
        assertFalse(verifyAssignmentResult.movesOngoing)

        // Wait for the assignment to complete
        waitForVerifyAssignment(cluster.adminClient, assignment, false,
            VerifyAssignmentResult(finalAssignment))

        assertEquals(unthrottledBrokerConfigs,
            describeBrokerLevelThrottles(unthrottledBrokerConfigs.keySet.toSeq))

        // Verify that partitions are removed from brokers no longer assigned
        verifyReplicaDeleted(topicPartition = foo0, replicaId = 2)
        verifyReplicaDeleted(topicPartition = bar0, replicaId = 1)
*/
    }

    class LogDirReassignment {
        final String json;
        final String currentDir;
        final String targetDir;

        public LogDirReassignment(String json, String currentDir, String targetDir) {
            this.json = json;
            this.currentDir = currentDir;
            this.targetDir = targetDir;
        }
    }

    private LogDirReassignment buildLogDirReassignment(TopicPartition topicPartition,
                                                       int brokerId,
                                                       List<Integer> replicas) throws ExecutionException, InterruptedException {

        DescribeLogDirsResult describeLogDirsResult = cluster.adminClient.describeLogDirs(
            IntStream.range(0, 4).boxed().collect(Collectors.toList()));

        BrokerDirs logDirInfo = new BrokerDirs(describeLogDirsResult, brokerId);
        assertTrue(logDirInfo.futureLogDirs.isEmpty());

        String currentDir = logDirInfo.curLogDirs.get(topicPartition);
        String newDir = logDirInfo.logDirs.stream().filter(dir -> !dir.equals(currentDir)).findFirst().get();

        List<String> logDirs = replicas.stream().map(replicaId -> {
            if (replicaId == brokerId)
                return "\"" + newDir + "\"";
            else
                return "\"any\"";
        }).collect(Collectors.toList());

        String reassignmentJson =
         " { \"version\": 1," +
         "  \"partitions\": [" +
         "    {" +
         "     \"topic\": \"" + topicPartition.topic() + "\"," +
         "     \"partition\": " + topicPartition.partition() + "," +
         "     \"replicas\": [" + replicas.stream().map(Object::toString).collect(Collectors.joining(",")) + "]," +
         "     \"log_dirs\": [" + String.join(",", logDirs) + "]" +
         "    }" +
         "   ]" +
         "  }";

        return new LogDirReassignment(reassignmentJson, currentDir, newDir);
    }



    private VerifyAssignmentResult runVerifyAssignment(Admin adminClient, String jsonString,
                                                       Boolean preserveThrottles) throws ExecutionException, InterruptedException, JsonProcessingException {
        System.out.println("==> verifyAssignment(adminClient, jsonString=" + jsonString);
        return verifyAssignment(adminClient, jsonString, preserveThrottles);
    }

    private void waitForVerifyAssignment(Admin adminClient,
                                        String jsonString,
                                        Boolean preserveThrottles,
                                        VerifyAssignmentResult expectedResult) {
        final VerifyAssignmentResult[] latestResult = {null};
        TestUtils.waitUntilTrue(
            () -> {
                try {
                    latestResult[0] = runVerifyAssignment(adminClient, jsonString, preserveThrottles);
                } catch (ExecutionException | InterruptedException | JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
                return expectedResult.equals(latestResult[0]);
            }, () -> "Timed out waiting for verifyAssignment result " + expectedResult + ".  " +
                "The latest result was " + latestResult[0], org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS, 10L);
    }

    private void runExecuteAssignment(Admin adminClient,
                                      Boolean additional,
                                      String reassignmentJson,
                                      Long interBrokerThrottle,
                                      Long replicaAlterLogDirsThrottle) throws ExecutionException, InterruptedException, JsonProcessingException {
        System.out.println("==> executeAssignment(adminClient, additional=" + additional + ", " +
            "reassignmentJson=" + reassignmentJson + ", " +
            "interBrokerThrottle=" + interBrokerThrottle + ", " +
            "replicaAlterLogDirsThrottle=" + replicaAlterLogDirsThrottle + "))");
        executeAssignment(adminClient, additional, reassignmentJson,
            interBrokerThrottle, replicaAlterLogDirsThrottle, 10000L, Time.SYSTEM);
    }

    private void runCancelAssignment(Admin adminClient, String jsonString, Boolean preserveThrottles) throws ExecutionException, InterruptedException, JsonProcessingException {
        System.out.println("==> cancelAssignment(adminClient, jsonString=" + jsonString);
        cancelAssignment(adminClient, jsonString, preserveThrottles, 10000L, Time.SYSTEM);
    }

    class BrokerDirs {
        final DescribeLogDirsResult result;
        final int brokerId;

        final Set<String> logDirs = new HashSet<>();
        final Map<TopicPartition, String> curLogDirs = new HashMap<>();
        final Map<TopicPartition, String> futureLogDirs = new HashMap<>();

        public BrokerDirs(DescribeLogDirsResult result, int brokerId) throws ExecutionException, InterruptedException {
            this.result = result;
            this.brokerId = brokerId;

            result.descriptions().get(brokerId).get().forEach((logDirName, logDirInfo) -> {
                logDirs.add(logDirName);
                logDirInfo.replicaInfos().forEach((part, info) -> {
                    if (info.isFuture()) {
                        futureLogDirs.put(part, logDirName);
                    } else {
                        curLogDirs.put(part, logDirName);
                    }
                });
            });
        }
    }

    class ReassignPartitionsTestCluster implements Closeable {
        private final Map<String, String> configOverrides;

        private final Map<Integer, Map<String, String>> brokerConfigOverrides;

        private final List<KafkaConfig> brokerConfigs = new ArrayList<>();

        private final Map<Integer, String> brokers = new HashMap<>(); {
            brokers.put(0, "rack0");
            brokers.put(1, "rack0");
            brokers.put(2, "rack1");
            brokers.put(3, "rack1");
            brokers.put(4, "rack1");
        }

        private final Map<String, List<List<Integer>>> topics = new HashMap<>(); {
            topics.put("foo", Arrays.asList(Arrays.asList(0, 1, 2), Arrays.asList(1, 2, 3)));
            topics.put("bar", Arrays.asList(Arrays.asList(3, 2, 1)));
            topics.put("baz", Arrays.asList(Arrays.asList(1, 0, 2), Arrays.asList(2, 0, 1), Arrays.asList(0, 2, 1)));
        }

        private List<KafkaBroker> servers = new ArrayList<>();

        private String brokerList;

        private Admin adminClient;

        public ReassignPartitionsTestCluster(Map<String, String> configOverrides, Map<Integer, Map<String, String>> brokerConfigOverrides) {
            this.configOverrides = configOverrides;
            this.brokerConfigOverrides = brokerConfigOverrides;

            brokers.forEach((brokerId, rack) -> {
                Properties config = TestUtils.createBrokerConfig(
                    brokerId,
                    zkConnectOrNull(),
                    false, // shorten test time
                    true,
                    TestUtils.RandomPort(),
                    scala.None$.empty(),
                    scala.None$.empty(),
                    scala.None$.empty(),
                    true,
                    false,
                    TestUtils.RandomPort(),
                    false,
                    TestUtils.RandomPort(),
                    false,
                    TestUtils.RandomPort(),
                    Some$.MODULE$.apply(rack),
                    3,
                    false,
                    1,
                    (short)1,
                    false);
                // shorter backoff to reduce test durations when no active partitions are eligible for fetching due to throttling
                config.setProperty(KafkaConfig.ReplicaFetchBackoffMsProp(), "100");
                // Don't move partition leaders automatically.
                config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp(), "false");
                config.setProperty(KafkaConfig.ReplicaLagTimeMaxMsProp(), "1000");
                configOverrides.forEach(config::setProperty);
                brokerConfigOverrides.getOrDefault(brokerId, Collections.emptyMap()).forEach(config::setProperty);

                brokerConfigs.add(new KafkaConfig(config));
            });
        }

        public void setup() throws ExecutionException, InterruptedException {
            createServers();
            createTopics();
        }

        public void createServers() {
            brokers.keySet().forEach(brokerId ->
                servers.add(createBroker(brokerConfigs.get(brokerId), Time.SYSTEM, true, scala.None$.empty()))
            );
        }

        public void createTopics() throws ExecutionException, InterruptedException {
            TestUtils.waitUntilBrokerMetadataIsPropagated(toSeq(servers), org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS);
            brokerList = TestUtils.plaintextBootstrapServers(toSeq(servers));

            adminClient = Admin.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList));

            adminClient.createTopics(topics.entrySet().stream().map(e -> {
                Map<Integer, List<Integer>> partMap = new HashMap<>();

                Iterator<List<Integer>> partsIter = e.getValue().iterator();
                int index = 0;
                while (partsIter.hasNext()) {
                    partMap.put(index, partsIter.next());
                    index++;
                }
                return new NewTopic(e.getKey(), partMap);
            }).collect(Collectors.toList())).all().get();
            topics.forEach((topicName, parts) -> {
                    TestUtils.waitForAllPartitionsMetadata(toSeq(servers), topicName, parts.size());
            });

            if (isKRaftTest()) {
                TestUtils.ensureConsistentKRaftMetadata(
                    toSeq(cluster.servers),
                    controllerServer(),
                    "Timeout waiting for controller metadata propagating to brokers"
                );
            }
        }

        public void produceMessages(String topic, int partition, int numMessages) {
            List<ProducerRecord<byte[], byte[]>> records = IntStream.range(0, numMessages).mapToObj(i ->
                new ProducerRecord<byte[], byte[]>(topic, partition,
                    null, new byte[10000])).collect(Collectors.toList());
            TestUtils.produceMessages(toSeq(servers), toSeq(records), -1);
        }

        @Override
        public void close() throws IOException {
            brokerList = null;
            Utils.closeQuietly(adminClient, "adminClient");
            adminClient = null;
            try {
                TestUtils.shutdownServers(toSeq(servers), true);
            } finally {
                servers.clear();
            }
        }
    }

    private static <T> Seq<T> toSeq(List<T> list) {
        return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }
}
