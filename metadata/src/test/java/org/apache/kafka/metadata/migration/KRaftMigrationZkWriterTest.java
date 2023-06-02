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
package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.image.AclsDelta;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.AclsImageTest;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClientQuotasImageTest;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationsDelta;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.ConfigurationsImageTest;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.ProducerIdsDelta;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ProducerIdsImageTest;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.ScramImageTest;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.image.TopicsImageTest;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.ProducerIdsBlock;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static org.apache.kafka.metadata.migration.KRaftMigrationZkWriter.DELETE_BROKER_CONFIG;
import static org.apache.kafka.metadata.migration.KRaftMigrationZkWriter.DELETE_TOPIC_CONFIG;
import static org.apache.kafka.metadata.migration.KRaftMigrationZkWriter.UPDATE_BROKER_CONFIG;
import static org.apache.kafka.metadata.migration.KRaftMigrationZkWriter.UPDATE_TOPIC_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class KRaftMigrationZkWriterTest {

    @Test
    public void testExtraneousZkPartitions() {
        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient() {
            @Override
            public void iterateTopics(EnumSet<TopicVisitorInterest> interests, TopicVisitor visitor) {
                Map<Integer, List<Integer>> assignments = new HashMap<>();
                assignments.put(0, Arrays.asList(2, 3, 4));
                assignments.put(1, Arrays.asList(3, 4, 5));
                assignments.put(2, Arrays.asList(2, 4, 5));
                assignments.put(3, Arrays.asList(1, 2, 3)); // This one is not in KRaft
                visitor.visitTopic("foo", TopicsImageTest.FOO_UUID, assignments);

                // Skip partition 1, visit 3 (the extra one)
                IntStream.of(0, 2, 3).forEach(partitionId -> {
                    visitor.visitPartition(
                        new TopicIdPartition(TopicsImageTest.FOO_UUID, new TopicPartition("foo", partitionId)),
                        TopicsImageTest.IMAGE1.getPartition(TopicsImageTest.FOO_UUID, partitionId)
                    );
                });

            }
        };

        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .setTopicMigrationClient(topicClient)
            .setConfigMigrationClient(configClient)
            .build();

        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        MetadataImage image = new MetadataImage(
            MetadataProvenance.EMPTY,
            FeaturesImage.EMPTY,
            ClusterImage.EMPTY,
            TopicsImageTest.IMAGE1,     // This includes "foo" with 3 partitions
            ConfigurationsImage.EMPTY,
            ClientQuotasImage.EMPTY,
            ProducerIdsImage.EMPTY,
            AclsImage.EMPTY,
            ScramImage.EMPTY
        );

        writer.handleSnapshot(image, (opType, opLog, operation) -> {
            operation.apply(ZkMigrationLeadershipState.EMPTY);
        });
        assertEquals(topicClient.updatedTopics.get("foo").size(), 3);
        assertEquals(topicClient.deletedTopicPartitions.get("foo"), Collections.singleton(3));
        assertEquals(topicClient.updatedTopicPartitions.get("foo"), Collections.singleton(1));
    }

    /**
     * If ZK is empty, ensure that the writer will sync all metadata from the MetadataImage to ZK
     */
    @Test
    public void testReconcileSnapshotEmptyZk() {

        // These test clients don't return any data in their iterates, so this simulates an empty ZK
        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient();
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingAclMigrationClient aclClient = new CapturingAclMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .setTopicMigrationClient(topicClient)
            .setConfigMigrationClient(configClient)
            .setAclMigrationClient(aclClient)
            .build();

        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        MetadataImage image = new MetadataImage(
            MetadataProvenance.EMPTY,
            FeaturesImage.EMPTY,        // Features are not used in ZK mode, so we don't migrate or dual-write them
            ClusterImage.EMPTY,         // Broker registrations are not dual-written
            TopicsImageTest.IMAGE1,
            ConfigurationsImageTest.IMAGE1,
            ClientQuotasImageTest.IMAGE1,
            ProducerIdsImageTest.IMAGE1,
            AclsImageTest.IMAGE1,
            ScramImageTest.IMAGE1
        );

        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
            (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleSnapshot(image, consumer);
        assertEquals(2, opCounts.remove("CreateTopic"));
        assertEquals(2, opCounts.remove("UpdateBrokerConfig"));
        assertEquals(1, opCounts.remove("UpdateProducerId"));
        assertEquals(4, opCounts.remove("UpdateAcl"));
        assertEquals(5, opCounts.remove("UpdateClientQuotas"));
        assertEquals(0, opCounts.size());

        assertEquals(2, topicClient.createdTopics.size());
        assertTrue(topicClient.createdTopics.contains("foo"));
        assertTrue(topicClient.createdTopics.contains("bar"));
        assertEquals("bar", configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "0")).get("foo"));
        assertEquals("quux", configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "0")).get("baz"));
        assertEquals("foobaz", configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "1")).get("foobar"));
        assertEquals(4, aclClient.updatedResources.size());
    }

    /**
     * Only return one of two topics in the ZK topic iterator, ensure that the topic client creates the missing topic
     */
    @Test
    public void testReconcileSnapshotTopics() {
        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient() {
            @Override
            public void iterateTopics(EnumSet<TopicVisitorInterest> interests, TopicVisitor visitor) {
                Map<Integer, List<Integer>> assignments = new HashMap<>();
                assignments.put(0, Arrays.asList(2, 3, 4));
                assignments.put(1, Arrays.asList(3, 4, 5));
                assignments.put(2, Arrays.asList(2, 4, 5));
                visitor.visitTopic("foo", TopicsImageTest.FOO_UUID, assignments);
            }
        };

        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingAclMigrationClient aclClient = new CapturingAclMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .setTopicMigrationClient(topicClient)
            .setConfigMigrationClient(configClient)
            .setAclMigrationClient(aclClient)
            .build();

        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        MetadataImage image = new MetadataImage(
            MetadataProvenance.EMPTY,
            FeaturesImage.EMPTY,
            ClusterImage.EMPTY,
            TopicsImageTest.IMAGE1,     // Two topics, foo and bar
            ConfigurationsImage.EMPTY,
            ClientQuotasImage.EMPTY,
            ProducerIdsImage.EMPTY,
            AclsImage.EMPTY,
            ScramImage.EMPTY
        );

        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
            (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleSnapshot(image, consumer);
        assertEquals(1, opCounts.remove("CreateTopic"));
        assertEquals(1, opCounts.remove("UpdatePartitions"));
        assertEquals(1, opCounts.remove("UpdateTopic"));
        assertEquals(0, opCounts.size());
        assertEquals("bar", topicClient.createdTopics.get(0));
    }

    @Test
    public void testDeleteTopicFromSnapshot() {
        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient() {
            @Override
            public void iterateTopics(EnumSet<TopicVisitorInterest> interests, TopicVisitor visitor) {
                visitor.visitTopic("spam", Uuid.randomUuid(), Collections.emptyMap());
            }
        };
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
                .setBrokersInZk(0)
                .setTopicMigrationClient(topicClient)
                .build();

        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleTopicsSnapshot(TopicsImage.EMPTY, consumer);
        assertEquals(1, opCounts.remove("DeleteTopic"));
        assertEquals(1, opCounts.remove("DeleteTopicConfig"));
        assertEquals(0, opCounts.size());
        assertEquals(Collections.singletonList("spam"), topicClient.deletedTopics);

        opCounts.clear();
        topicClient.reset();
        writer.handleTopicsSnapshot(TopicsImageTest.IMAGE1, consumer);
        assertEquals(1, opCounts.remove("DeleteTopic"));
        assertEquals(1, opCounts.remove("DeleteTopicConfig"));
        assertEquals(2, opCounts.remove("CreateTopic"));
        assertEquals(0, opCounts.size());
        assertEquals(Collections.singletonList("spam"), topicClient.deletedTopics);
        assertEquals(Arrays.asList("foo", "bar"), topicClient.createdTopics);
    }

    @Test
    public void testUpdatePartitionsFromSnapshot() {
        Uuid topicId = Uuid.randomUuid();
        Map<Integer, PartitionRegistration> partitionMap = new HashMap<>();
        partitionMap.put(0, new PartitionRegistration(new int[]{2, 3, 4}, new int[]{2, 3, 4}, new int[]{}, new int[]{}, 2, LeaderRecoveryState.RECOVERED, 0, -1));
        partitionMap.put(1, new PartitionRegistration(new int[]{3, 4, 5}, new int[]{3, 4, 5}, new int[]{}, new int[]{}, 3, LeaderRecoveryState.RECOVERED, 0, -1));

        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient() {
            @Override
            public void iterateTopics(EnumSet<TopicVisitorInterest> interests, TopicVisitor visitor) {
                Map<Integer, List<Integer>> assignments = new HashMap<>();
                assignments.put(0, Arrays.asList(2, 3, 4));
                assignments.put(1, Arrays.asList(3, 4, 5));
                visitor.visitTopic("spam", topicId, assignments);
                visitor.visitPartition(new TopicIdPartition(topicId, new TopicPartition("spam", 0)), partitionMap.get(0));
                visitor.visitPartition(new TopicIdPartition(topicId, new TopicPartition("spam", 1)), partitionMap.get(1));
            }
        };
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
                .setBrokersInZk(0)
                .setTopicMigrationClient(topicClient)
                .build();

        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        TopicsDelta delta = new TopicsDelta(TopicsImage.EMPTY);
        delta.replay(new TopicRecord().setTopicId(topicId).setName("spam"));
        delta.replay((PartitionRecord) partitionMap.get(0).toRecord(topicId, 0).message());
        delta.replay((PartitionRecord) partitionMap.get(1).toRecord(topicId, 1).message());
        TopicsImage image = delta.apply();

        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleTopicsSnapshot(image, consumer);
        assertEquals(0, opCounts.size(), "No operations expected since the data is the same");

        delta = new TopicsDelta(image);
        delta.replay(new PartitionChangeRecord().setTopicId(topicId).setPartitionId(0).setIsr(Arrays.asList(2, 3)));
        delta.replay(new PartitionChangeRecord().setTopicId(topicId).setPartitionId(1).setReplicas(Arrays.asList(3, 4, 5)).setLeader(3));
        image = delta.apply();
        writer.handleTopicsSnapshot(image, consumer);
        assertEquals(1, opCounts.remove("UpdatePartitions"));
        assertEquals(0, opCounts.size());
    }

    @Test
    public void testBrokerConfigDelta() {
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .setConfigMigrationClient(configClient)
            .build();
        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);
        ConfigurationsDelta delta = new ConfigurationsDelta(ConfigurationsImage.EMPTY);
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("b0").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("b0").setName("spam").setValue(null));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("topic-0").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("topic-1").setName("foo").setValue(null));

        ConfigurationsImage image = delta.apply();
        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleConfigsDelta(image, delta, consumer);
        assertEquals(
            Collections.singletonMap("foo", "bar"),
            configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "b0"))
        );
        assertEquals(
            Collections.singletonMap("foo", "bar"),
            configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, "topic-0"))
        );
        assertTrue(
            configClient.deletedResources.contains(new ConfigResource(ConfigResource.Type.TOPIC, "topic-1"))
        );
    }

    @Test
    public void testBrokerConfigSnapshot() {
        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient();
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient() {
            @Override
            public void iterateBrokerConfigs(BiConsumer<String, Map<String, String>> configConsumer) {
                Map<String, String> b0 = new HashMap<>();
                b0.put("foo", "bar");
                b0.put("spam", "eggs");
                configConsumer.accept("0", b0);
                configConsumer.accept("1", Collections.singletonMap("foo", "bar"));
                configConsumer.accept("3", Collections.singletonMap("foo", "bar"));
            }
        };
        CapturingAclMigrationClient aclClient = new CapturingAclMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
                .setBrokersInZk(0)
                .setTopicMigrationClient(topicClient)
                .setConfigMigrationClient(configClient)
                .setAclMigrationClient(aclClient)
                .build();
        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        ConfigurationsDelta delta = new ConfigurationsDelta(ConfigurationsImage.EMPTY);
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("0").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("1").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.BROKER.id()).setResourceName("2").setName("foo").setValue("bar"));

        ConfigurationsImage image = delta.apply();
        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
            (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleConfigsSnapshot(image, consumer);

        assertTrue(configClient.deletedResources.contains(new ConfigResource(ConfigResource.Type.BROKER, "3")),
            "Broker 3 is not in the ConfigurationsImage, it should get deleted");

        assertEquals(
            Collections.singletonMap("foo", "bar"),
            configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "0")),
            "Broker 0 only has foo=bar in image, should overwrite the ZK config");

        assertFalse(configClient.writtenConfigs.containsKey(new ConfigResource(ConfigResource.Type.BROKER, "1")),
            "Broker 1 config is the same in image, so no write should happen");

        assertEquals(
            Collections.singletonMap("foo", "bar"),
            configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, "2")),
            "Broker 2 not present in ZK, should see an update");

        assertEquals(2, opCounts.get(UPDATE_BROKER_CONFIG));
        assertEquals(1, opCounts.get(DELETE_BROKER_CONFIG));
    }

    @Test
    public void testTopicConfigSnapshot() {
        CapturingTopicMigrationClient topicClient = new CapturingTopicMigrationClient();
        CapturingConfigMigrationClient configClient = new CapturingConfigMigrationClient() {
            @Override
            public void iterateTopicConfigs(BiConsumer<String, Map<String, String>> configConsumer) {
                Map<String, String> topic0 = new HashMap<>();
                topic0.put("foo", "bar");
                topic0.put("spam", "eggs");
                configConsumer.accept("topic-0", topic0);
                configConsumer.accept("topic-1", Collections.singletonMap("foo", "bar"));
                configConsumer.accept("topic-3", Collections.singletonMap("foo", "bar"));
            }
        };
        CapturingAclMigrationClient aclClient = new CapturingAclMigrationClient();
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .setTopicMigrationClient(topicClient)
            .setConfigMigrationClient(configClient)
            .setAclMigrationClient(aclClient)
            .build();
        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        ConfigurationsDelta delta = new ConfigurationsDelta(ConfigurationsImage.EMPTY);
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("topic-0").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("topic-1").setName("foo").setValue("bar"));
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("topic-2").setName("foo").setValue("bar"));

        ConfigurationsImage image = delta.apply();
        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleConfigsSnapshot(image, consumer);

        assertTrue(configClient.deletedResources.contains(new ConfigResource(ConfigResource.Type.TOPIC, "topic-3")),
                "Topic topic-3 is not in the ConfigurationsImage, it should get deleted");

        assertEquals(
                Collections.singletonMap("foo", "bar"),
                configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, "topic-0")),
                "Topic topic-0 only has foo=bar in image, should overwrite the ZK config");

        assertFalse(configClient.writtenConfigs.containsKey(new ConfigResource(ConfigResource.Type.TOPIC, "topic-1")),
                "Topic topic-1 config is the same in image, so no write should happen");

        assertEquals(
                Collections.singletonMap("foo", "bar"),
                configClient.writtenConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, "topic-2")),
                "Topic topic-2 not present in ZK, should see an update");

        assertEquals(2, opCounts.get(UPDATE_TOPIC_CONFIG));
        assertEquals(1, opCounts.get(DELETE_TOPIC_CONFIG));
    }

    @Test
    public void testInvalidConfigSnapshot() {
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder().build();
        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);
        ConfigurationsDelta delta = new ConfigurationsDelta(ConfigurationsImage.EMPTY);
        delta.replay(new ConfigRecord().setResourceType((byte) 99).setResourceName("resource").setName("foo").setValue("bar"));

        ConfigurationsImage image = delta.apply();
        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
            (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        assertThrows(RuntimeException.class, () -> writer.handleConfigsSnapshot(image, consumer),
            "Should throw due to invalid resource in image");
    }

    @Test
    public void testProducerIdSnapshot() {
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .build();
        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        migrationClient.setReadProducerId(new ProducerIdsBlock(0, 100L, 1000));

        {
            // No change
            ProducerIdsImage image = new ProducerIdsImage(1100);
            Map<String, Integer> opCounts = new HashMap<>();
            KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                    (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
            writer.handleProducerIdSnapshot(image, consumer);
            assertEquals(0, opCounts.size());
        }

        {
            // KRaft differs from ZK
            ProducerIdsImage image = new ProducerIdsImage(2000);
            Map<String, Integer> opCounts = new HashMap<>();
            KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                    (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
            writer.handleProducerIdSnapshot(image, consumer);
            assertEquals(1, opCounts.size());
            assertEquals(2000, migrationClient.capturedProducerId);
        }

        {
            // "Empty" state in ZK (shouldn't really happen, but good to check)
            ProducerIdsImage image = new ProducerIdsImage(2000);
            migrationClient.setReadProducerId(ProducerIdsBlock.EMPTY);
            Map<String, Integer> opCounts = new HashMap<>();
            KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
            writer.handleProducerIdSnapshot(image, consumer);
            assertEquals(1, opCounts.size());
            assertEquals(2000, migrationClient.capturedProducerId);
        }

        {
            // No state in ZK
            ProducerIdsImage image = new ProducerIdsImage(2000);
            migrationClient.setReadProducerId(null);
            Map<String, Integer> opCounts = new HashMap<>();
            KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                    (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
            writer.handleProducerIdSnapshot(image, consumer);
            assertEquals(1, opCounts.size());
            assertEquals(2000, migrationClient.capturedProducerId);
        }
    }

    @Test
    public void testProducerIdDelta() {
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setBrokersInZk(0)
            .build();
        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        migrationClient.setReadProducerId(new ProducerIdsBlock(0, 100L, 1000));

        // No change
        ProducerIdsDelta delta = new ProducerIdsDelta(ProducerIdsImage.EMPTY);
        delta.replay(new ProducerIdsRecord().setBrokerId(0).setBrokerEpoch(20).setNextProducerId(2000));

        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleProducerIdDelta(delta, consumer);
        assertEquals(1, opCounts.size());
        assertEquals(2000, migrationClient.capturedProducerId);
    }

    @Test
    public void testAclSnapshot() {
        ResourcePattern resource1 = new ResourcePattern(ResourceType.TOPIC, "foo-" + Uuid.randomUuid(), PatternType.LITERAL);
        ResourcePattern resource2 = new ResourcePattern(ResourceType.TOPIC, "bar-" + Uuid.randomUuid(), PatternType.LITERAL);
        ResourcePattern resource3 = new ResourcePattern(ResourceType.TOPIC, "baz-" + Uuid.randomUuid(), PatternType.LITERAL);

        KafkaPrincipal principal1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        KafkaPrincipal principal2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob");
        AccessControlEntry acl1Resource1 = new AccessControlEntry(principal1.toString(), "*", AclOperation.WRITE, AclPermissionType.ALLOW);
        AccessControlEntry acl1Resource2 = new AccessControlEntry(principal2.toString(), "*", AclOperation.READ, AclPermissionType.ALLOW);

        CapturingAclMigrationClient aclClient = new CapturingAclMigrationClient() {
            @Override
            public void iterateAcls(BiConsumer<ResourcePattern, Set<AccessControlEntry>> aclConsumer) {
                aclConsumer.accept(resource1, Collections.singleton(acl1Resource1));
                aclConsumer.accept(resource2, Collections.singleton(acl1Resource2));
            }
        };
        CapturingMigrationClient migrationClient = CapturingMigrationClient.newBuilder()
            .setAclMigrationClient(aclClient)
            .build();
        KRaftMigrationZkWriter writer = new KRaftMigrationZkWriter(migrationClient);

        // Create an ACL for a new resource.
        AclsDelta delta = new AclsDelta(AclsImage.EMPTY);
        AccessControlEntryRecord acl1Resource3 = new AccessControlEntryRecord()
            .setId(Uuid.randomUuid())
            .setHost("192.168.10.1")
            .setOperation(AclOperation.READ.code())
            .setPrincipal("*")
            .setPermissionType(AclPermissionType.ALLOW.code())
            .setPatternType(resource3.patternType().code())
            .setResourceName(resource3.name())
            .setResourceType(resource3.resourceType().code());
        // The equivalent ACE
        AccessControlEntry ace1Resource3 = new AccessControlEntry("*", "192.168.10.1", AclOperation.READ, AclPermissionType.ALLOW);
        delta.replay(acl1Resource3);

        // Change an ACL for existing resource.
        AccessControlEntryRecord acl2Resource1 = new AccessControlEntryRecord()
            .setId(Uuid.randomUuid())
            .setHost("192.168.15.1")
            .setOperation(AclOperation.WRITE.code())
            .setPrincipal(principal1.toString())
            .setPermissionType(AclPermissionType.ALLOW.code())
            .setPatternType(resource1.patternType().code())
            .setResourceName(resource1.name())
            .setResourceType(resource1.resourceType().code());
        // The equivalent ACE
        AccessControlEntry ace1Resource1 = new AccessControlEntry(principal1.toString(), "192.168.15.1", AclOperation.WRITE, AclPermissionType.ALLOW);
        delta.replay(acl2Resource1);

        // Do not add anything for resource 2 in the delta.
        AclsImage image = delta.apply();
        Map<String, Integer> opCounts = new HashMap<>();
        KRaftMigrationOperationConsumer consumer = KRaftMigrationDriver.countingOperationConsumer(opCounts,
                (logMsg, operation) -> operation.apply(ZkMigrationLeadershipState.EMPTY));
        writer.handleAclsSnapshot(image, consumer);

        assertTrue(aclClient.deletedResources.contains(resource2));
        assertEquals(Collections.singleton(ace1Resource1), aclClient.updatedResources.get(resource1));
        assertEquals(Collections.singleton(ace1Resource3), aclClient.updatedResources.get(resource3));
    }
}
