/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;

@Category({IntegrationTest.class})
public class RackAwarenessIntegrationTest {
    private static final int NUM_BROKERS = 1;

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @Rule
    public TestName testName = new TestName();

    private static final String INPUT_TOPIC = "input-topic";
    private static final String TAG_ZONE = "zone";
    private static final String TAG_CLUSTER = "cluster";

    private List<KafkaStreamsWithConfiguration> kafkaStreamsInstances;
    private Properties baseConfiguration;

    @BeforeClass
    public static void createTopics() throws Exception {
        CLUSTER.start();
        CLUSTER.createTopic(INPUT_TOPIC, 6, 1);
    }

    @Before
    public void setup() {
        kafkaStreamsInstances = new ArrayList<>();
        baseConfiguration = new Properties();
        final String safeTestName = safeUniqueTestName(getClass(), testName);
        final String applicationId = "app-" + safeTestName;
        baseConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        baseConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        baseConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        baseConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        baseConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
    }

    @After
    public void cleanup() throws IOException {
        for (final KafkaStreamsWithConfiguration kafkaStreamsWithConfiguration : kafkaStreamsInstances) {
            kafkaStreamsWithConfiguration.kafkaStreams.close(Duration.ofMillis(IntegrationTestUtils.DEFAULT_TIMEOUT));
            IntegrationTestUtils.purgeLocalStreamsState(kafkaStreamsWithConfiguration.configuration);
        }
        kafkaStreamsInstances.clear();
    }

    @Test
    public void shouldDistributeStandbyReplicasBasedOnClientTags() throws Exception {
        final Topology topology = createStatefulTopology();
        final int numberOfStandbyReplicas = 1;

        createAndStart(topology, buildClientTags("eu-central-1a", "k8s-cluster-1"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(topology, buildClientTags("eu-central-1a", "k8s-cluster-1"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(topology, buildClientTags("eu-central-1a", "k8s-cluster-1"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);

        createAndStart(topology, buildClientTags("eu-central-1b", "k8s-cluster-1"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(topology, buildClientTags("eu-central-1b", "k8s-cluster-1"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(topology, buildClientTags("eu-central-1b", "k8s-cluster-1"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);

        waitUntilRackAwareTaskDistributionIsReached(TAG_ZONE);
    }

    @Test
    public void shouldDistributeStandbyReplicasOverMultipleClientTags() throws Exception {
        final Topology topology = createStatefulTopology();
        final int numberOfStandbyReplicas = 2;

        createAndStart(topology, buildClientTags("eu-central-1a", "k8s-cluster-1"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(topology, buildClientTags("eu-central-1b", "k8s-cluster-1"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(topology, buildClientTags("eu-central-1c", "k8s-cluster-1"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);

        createAndStart(topology, buildClientTags("eu-central-1a", "k8s-cluster-2"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(topology, buildClientTags("eu-central-1b", "k8s-cluster-2"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(topology, buildClientTags("eu-central-1c", "k8s-cluster-2"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);

        createAndStart(topology, buildClientTags("eu-central-1a", "k8s-cluster-3"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(topology, buildClientTags("eu-central-1b", "k8s-cluster-3"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);
        createAndStart(topology, buildClientTags("eu-central-1c", "k8s-cluster-3"), asList(TAG_ZONE, TAG_CLUSTER), numberOfStandbyReplicas);


        waitUntilRackAwareTaskDistributionIsReached(TAG_ZONE, TAG_CLUSTER);
    }

    private void waitUntilRackAwareTaskDistributionIsReached(final String... tagsToCheck) throws Exception {
        final TestCondition condition = () -> {

            final List<TaskClientTagDistribution> tasksClientTagDistributions = getTasksClientTagDistributions();

            if (tasksClientTagDistributions.isEmpty()) {
                return false;
            }

            return tasksClientTagDistributions.stream().allMatch(taskClientTagDistribution -> {
                final Map<String, String> activeTaskClientTags = taskClientTagDistribution.activeTaskClientTags.clientTags;

                return verifyTasksDistribution(taskClientTagDistribution.standbyTasksClientTags,
                                               activeTaskClientTags,
                                               asList(tagsToCheck));
            });
        };

        TestUtils.waitForCondition(
            condition,
            IntegrationTestUtils.DEFAULT_TIMEOUT,
            "Rack aware task distribution couldn't be reached on " +
            "client tags [" + Arrays.toString(tagsToCheck) + "]."
        );
    }

    private static boolean verifyTasksDistribution(final List<TaskClientTags> standbyTasks,
                                                   final Map<String, String> activeTaskClientTags,
                                                   final List<String> tagsToCheck) {
        return tagsAmongstStandbyTasksAreDifferent(standbyTasks, tagsToCheck)
               && tagsAmongstActiveAndStandbyTasksAreDifferent(standbyTasks, activeTaskClientTags, tagsToCheck);
    }

    private static boolean tagsAmongstActiveAndStandbyTasksAreDifferent(final List<TaskClientTags> standbyTasks,
                                                                        final Map<String, String> activeTaskClientTags,
                                                                        final List<String> tagsToCheck) {
        return standbyTasks.stream().allMatch(standbyTask -> tagsToCheck.stream().noneMatch(tag -> activeTaskClientTags.get(tag).equals(standbyTask.clientTags.get(tag))));
    }

    private static boolean tagsAmongstStandbyTasksAreDifferent(final List<TaskClientTags> standbyTasks, final List<String> tagsToCheck) {
        final Map<String, Integer> statistics = new HashMap<>();

        for (final TaskClientTags standbyTask : standbyTasks) {
            for (final String tag : tagsToCheck) {
                final String tagValue = standbyTask.clientTags.get(tag);
                final Integer tagValueOccurrence = statistics.getOrDefault(tagValue, 0);
                statistics.put(tagValue, tagValueOccurrence + 1);
            }
        }

        return statistics.values().stream().noneMatch(occurrence -> occurrence > 1);
    }

    private static Topology createStatefulTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String stateStoreName = "myTransformState";

        final StoreBuilder<KeyValueStore<Integer, Integer>> keyValueStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stateStoreName),
            Serdes.Integer(),
            Serdes.Integer()
        );

        builder.addStateStore(keyValueStoreBuilder);

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), Serdes.Integer()))
               .transform(() -> new Transformer<Integer, Integer, KeyValue<Integer, Integer>>() {
                   @Override
                   public void init(final ProcessorContext context) {
                   }

                   @Override
                   public KeyValue<Integer, Integer> transform(final Integer key, final Integer value) {
                       return null;
                   }

                   @Override
                   public void close() {
                   }
               }, stateStoreName);

        return builder.build();
    }

    private List<TaskClientTagDistribution> getTasksClientTagDistributions() {
        final List<TaskClientTagDistribution> taskClientTags = new ArrayList<>();

        for (final KafkaStreamsWithConfiguration kafkaStreamsInstance : kafkaStreamsInstances) {
            final StreamsConfig config = new StreamsConfig(kafkaStreamsInstance.configuration);
            for (final ThreadMetadata localThreadsMetadata : kafkaStreamsInstance.kafkaStreams.metadataForLocalThreads()) {
                localThreadsMetadata.activeTasks().forEach(activeTask -> {
                    final TaskId activeTaskId = activeTask.taskId();
                    final Map<String, String> clientTags = config.getClientTags();
                    final List<TaskClientTags> standbyTasks = findStandbysForActiveTask(activeTaskId);

                    final TaskClientTags activeTaskView = new TaskClientTags(activeTaskId, clientTags);
                    taskClientTags.add(new TaskClientTagDistribution(activeTaskView, standbyTasks));
                });

            }
        }

        return taskClientTags;
    }

    private List<TaskClientTags> findStandbysForActiveTask(final TaskId taskId) {
        final List<TaskClientTags> standbyTasks = new ArrayList<>();

        for (final KafkaStreamsWithConfiguration kafkaStreamsInstance : kafkaStreamsInstances) {
            for (final ThreadMetadata localThreadsMetadata : kafkaStreamsInstance.kafkaStreams.metadataForLocalThreads()) {
                localThreadsMetadata.standbyTasks().forEach(standbyTask -> {
                    final TaskId standbyTaskId = standbyTask.taskId();
                    if (taskId.equals(standbyTaskId)) {
                        final StreamsConfig config = new StreamsConfig(kafkaStreamsInstance.configuration);
                        standbyTasks.add(new TaskClientTags(standbyTaskId, config.getClientTags()));
                    }
                });
            }
        }

        return standbyTasks;
    }

    private static Map<String, String> buildClientTags(final String zone, final String cluster) {
        final Map<String, String> clientTags = new HashMap<>();

        clientTags.put(TAG_ZONE, zone);
        clientTags.put(TAG_CLUSTER, cluster);

        return clientTags;
    }

    private void createAndStart(final Topology topology,
                                final Map<String, String> clientTags,
                                final List<String> rackAwareAssignmentTags,
                                final int numberOfStandbyReplicas) {
        final Properties streamsConfiguration = createStreamsConfiguration(clientTags, rackAwareAssignmentTags, numberOfStandbyReplicas);
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfiguration);

        kafkaStreamsInstances.add(new KafkaStreamsWithConfiguration(streamsConfiguration, kafkaStreams));

        kafkaStreams.start();
    }

    private Properties createStreamsConfiguration(final Map<String, String> clientTags,
                                                  final List<String> rackAwareAssignmentTags,
                                                  final int numStandbyReplicas) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.putAll(baseConfiguration);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
        streamsConfiguration.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, String.join(",", rackAwareAssignmentTags));
        clientTags.forEach((key, value) -> streamsConfiguration.put(StreamsConfig.clientTagPrefix(key), value));
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(String.join("-", clientTags.values())).getPath());
        return streamsConfiguration;
    }

    private static final class KafkaStreamsWithConfiguration {
        private final Properties configuration;
        private final KafkaStreams kafkaStreams;

        KafkaStreamsWithConfiguration(final Properties configuration, final KafkaStreams kafkaStreams) {
            this.configuration = configuration;
            this.kafkaStreams = kafkaStreams;
        }
    }

    private static final class TaskClientTagDistribution {
        private final TaskClientTags activeTaskClientTags;
        private final List<TaskClientTags> standbyTasksClientTags;

        TaskClientTagDistribution(final TaskClientTags activeTaskClientTags, final List<TaskClientTags> standbyTasksClientTags) {
            this.activeTaskClientTags = activeTaskClientTags;
            this.standbyTasksClientTags = standbyTasksClientTags;
        }

        @Override
        public String toString() {
            return "TaskDistribution{" +
                   "activeTaskClientTagsView=" + activeTaskClientTags +
                   ", standbyTasks=" + standbyTasksClientTags +
                   '}';
        }
    }

    private static final class TaskClientTags {
        private final TaskId taskId;
        private final Map<String, String> clientTags;

        TaskClientTags(final TaskId taskId, final Map<String, String> clientTags) {
            this.taskId = taskId;
            this.clientTags = clientTags;
        }

        @Override
        public String toString() {
            return "TaskClientTags{" +
                   "taskId=" + taskId +
                   ", clientTags=" + clientTags +
                   '}';
        }
    }
}