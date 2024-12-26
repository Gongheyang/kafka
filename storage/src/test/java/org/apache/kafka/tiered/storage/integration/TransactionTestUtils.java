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
package org.apache.kafka.tiered.storage.integration;

import kafka.server.HostedPartition;
import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.storage.internals.log.ProducerStateEntry;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tiered.storage.utils.BrokerLocalStorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.jdk.javaapi.CollectionConverters;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTION_TIMEOUT_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.test.api.Type.CO_KRAFT;
import static org.apache.kafka.common.test.api.Type.KRAFT;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG;
import static org.apache.kafka.server.common.Features.TRANSACTION_VERSION;
import static org.apache.kafka.server.config.ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.STORAGE_WAIT_TIMEOUT_SEC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TransactionTestUtils {

    public static final String TOPIC1 = "topic1";
    public static final String TOPIC2 = "topic2";
    public static final int NUM_PARTITIONS = 4;
    private static final String TRANSACTION_STATUS_KEY = "transactionStatus";
    private static final String COMMITTED_KEY = "committed";

    static List<ClusterConfig> generator(Properties prop) {
        return List.of(ClusterConfig.defaultBuilder()
                .setBrokers(3)
                .setAutoStart(true)
                .setTypes(Set.of(KRAFT, CO_KRAFT))
                .setServerProperties(serverConfig(prop))
                .setTags(List.of("kraftGroupCoordinator"))
                .build());
    }

    static List<ClusterConfig> generateTV2Disabled(Properties prop) {
        return generator(prop, false);
    }
    
    static List<ClusterConfig> generateTV2Enabled(Properties prop) {
        return generator(prop, true);
    }

    static void createTopic(
            ClusterInstance cluster,
            String topName,
            int numPartitions,
            int size,
            Map<String, String> config
    ) {
        try (Admin adminClient = cluster.createAdminClient()) {
            adminClient.createTopics(List.of(new NewTopic(topName, numPartitions, (short) size).configs(config)));
        }
    }

    static void createTopics(ClusterInstance cluster, Map<String, String> topicConfig) {
        try (Admin adminClient = cluster.createAdminClient()) {
            adminClient.createTopics(List.of(
                    new NewTopic(TOPIC1, NUM_PARTITIONS, (short) 3).configs(topicConfig),
                    new NewTopic(TOPIC2, NUM_PARTITIONS, (short) 3).configs(topicConfig))
            );
        }
    }

    static KafkaProducer<byte[], byte[]> createDefaultTransactionalProducer(ClusterInstance cluster) {
        return createTransactionalProducer(cluster, "transactional-producer",
                2000, 2000, 4000, 1000);
    }

    static KafkaProducer<byte[], byte[]> createTransactionalProducer(
            ClusterInstance cluster,
            String transactionalId,
            int transactionTimeoutMs,
            int maxBlockMs,
            int deliveryTimeoutMs,
            int requestTimeoutMs
    ) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ACKS_CONFIG, "all");
        props.put(BATCH_SIZE_CONFIG, 16384);
        props.put(TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        props.put(MAX_BLOCK_MS_CONFIG, maxBlockMs);
        props.put(DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        props.put(REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        return new KafkaProducer<>(props);
    }

    static KafkaConsumer<byte[], byte[]> createDefaultReadCommittedConsumer(
            ClusterInstance cluster, 
            GroupProtocol groupProtocol
    ) {
        return createReadCommittedConsumer(cluster, "transactional-group", 100, groupProtocol, new Properties());
    }

    static KafkaConsumer<byte[], byte[]> createReadCommittedConsumer(
            ClusterInstance cluster,
            String group,
            int maxPollRecords,
            GroupProtocol groupProtocol,
            Properties props
    ) {
        var prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        prop.put(GROUP_PROTOCOL_CONFIG, groupProtocol.name);
        prop.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(GROUP_ID_CONFIG, group);
        prop.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        prop.put(MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        prop.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        prop.putAll(props);
        return new KafkaConsumer<>(prop, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    static KafkaConsumer<byte[], byte[]> createReadUncommittedConsumer(ClusterInstance cluster, GroupProtocol groupProtocol) {
        var prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        prop.put(GROUP_PROTOCOL_CONFIG, groupProtocol.name);
        prop.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        prop.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        prop.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(GROUP_ID_CONFIG, "non-transactional-group");
        prop.put(ENABLE_AUTO_COMMIT_CONFIG, true);
        prop.put(MAX_POLL_RECORDS_CONFIG, 500);
        prop.put(ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        return new KafkaConsumer<>(prop);
    }

    static ProducerRecord<byte[], byte[]> producerRecordWithExpectedTransactionStatus(
            String topic,
            Integer partition,
            String key,
            String value,
            boolean willBeCommitted
    ) {
        return producerRecordWithExpectedTransactionStatus(topic, partition, key, value, new AtomicBoolean(willBeCommitted));
    }

    static ProducerRecord<byte[], byte[]> producerRecordWithExpectedTransactionStatus(
            String topic,
            Integer partition,
            String key,
            String value,
            AtomicBoolean willBeCommitted
    ) {
        var header = new Header() {
            @Override
            public String key() {
                return TRANSACTION_STATUS_KEY;
            }

            @Override
            public byte[] value() {
                return willBeCommitted.get() ? COMMITTED_KEY.getBytes(UTF_8) : "aborted".getBytes(UTF_8);
            }
        };
        return new ProducerRecord<>(topic, partition, key.getBytes(), value.getBytes(), singleton(header));
    }

    static void verifyLogStartOffsets(
            ClusterInstance cluster,
            Map<TopicPartition, Integer> partitionStartOffsets
    ) throws InterruptedException {
        Collection<KafkaBroker> brokers = cluster.brokers().values();
        Map<Integer, Long> offsets = new HashMap<>();
        waitForCondition(() -> {
            for (KafkaBroker broker : brokers) {
                for (Map.Entry<TopicPartition, Integer> entry : partitionStartOffsets.entrySet()) {
                    long offset = broker.replicaManager().localLog(entry.getKey()).get().localLogStartOffset();
                    offsets.put(broker.config().brokerId(), offset);
                    return entry.getValue() == offset;
                }
            }
            return false;
        }, "log start offset doesn't change to the expected position: " + partitionStartOffsets + ", current position: " + offsets);
    }

    static <K, V> List<ConsumerRecord<K, V>> consumeRecords(
            Consumer<K, V> consumer,
            int numRecords
    ) throws InterruptedException {
        List<ConsumerRecord<K, V>> records = pollUntilAtLeastNumRecords(consumer, numRecords);
        assertEquals(numRecords, records.size(), "Consumed more records than expected");
        return records;
    }

    static <K, V> List<ConsumerRecord<K, V>> pollUntilAtLeastNumRecords(
            Consumer<K, V> consumer,
            int numRecords
    ) throws InterruptedException {
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100)).forEach(records::add);
            return records.size() >= numRecords;
        }, DEFAULT_MAX_WAIT_MS, format("Expected %d records will be consumed", numRecords));
        return records;
    }

    static String assertCommittedAndGetValue(ConsumerRecord<byte[], byte[]> record) {
        var header = record.headers().headers(TRANSACTION_STATUS_KEY).iterator().next();
        if (header != null) {
            assertEquals(COMMITTED_KEY, new String(header.value(), UTF_8), "Got " + new String(header.value(), UTF_8) + " but expected the value to indicate committed status.");
        } else {
            throw new RuntimeException("expected the record header to include an expected transaction status, but received nothing.");
        }
        return recordValueAsString(record);
    }

    static String recordValueAsString(ConsumerRecord<byte[], byte[]> record) {
        return new String(record.value(), UTF_8);
    }

    /**
     * The basic plan for the test is as follows:
     * 1. Seed topic1 with 500 unique, numbered, messages.
     * 2. Run a consume/process/produce loop to transactionally copy messages from topic1 to topic2 and commit
     * offsets as part of the transaction.
     * 3. Randomly abort transactions in step2.
     * 4. Validate that we have 500 unique committed messages in topic2. If the offsets were committed properly with the
     * transactions, we should not have any duplicates or missing messages since we should process in the input
     * messages exactly once.
     */
    static void sendOffset(ClusterInstance cluster, OffsetHandler handler, boolean isSkip, GroupProtocol groupProtocol) throws Exception {
        var consumerGroupId = "foobar-consumer-group";
        var numSeedMessages = 500;
        seedTopicWithNumberedRecords(numSeedMessages, cluster);

        try (var producer = createDefaultTransactionalProducer(cluster);
             var consumer = createReadCommittedConsumer(cluster, consumerGroupId, numSeedMessages / 4, groupProtocol, new Properties())
        ) {
            consumer.subscribe(List.of(TOPIC1));
            producer.initTransactions();

            AtomicBoolean shouldCommit = new AtomicBoolean(false);
            int recordsProcessed = 0;

            while (recordsProcessed < numSeedMessages) {
                var records = pollUntilAtLeastNumRecords(consumer, Math.min(10, numSeedMessages - recordsProcessed));

                producer.beginTransaction();
                shouldCommit.set(!shouldCommit.get());

                records.forEach(record -> {
                    var key = new String(record.key(), UTF_8);
                    var value = new String(record.value(), UTF_8);
                    producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, key, value, shouldCommit));
                });

                handler.commit(producer, consumerGroupId, consumer);

                if (shouldCommit.get()) {
                    producer.commitTransaction();
                    recordsProcessed += records.size();
                } else {
                    producer.abortTransaction();
                    resetToCommittedPositions(consumer);
                }
            }
        }

        Set<TopicPartition> partitions = new HashSet<>();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            partitions.add(new TopicPartition(TOPIC2, i));
        }

        maybeWaitForAtLeastOneSegmentUpload(cluster, partitions, isSkip);

        // In spite of random aborts, we should still have exactly 500 messages in topic2. I.e. we should not
        // re-copy or miss any messages from topic1, since the consumed offsets were committed transactionally.
        try (KafkaConsumer<byte[], byte[]> verifyingConsumer = createReadCommittedConsumer(cluster,
                "transactional-group", 100, groupProtocol, new Properties())) {
            verifyingConsumer.subscribe(Collections.singleton(TOPIC2));
            List<String> valueList = pollUntilAtLeastNumRecords(verifyingConsumer, numSeedMessages)
                    .stream()
                    .map(TransactionTestUtils::assertCommittedAndGetValue)
                    .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

            Set<String> valueSet = new HashSet<>(valueList);

            assertEquals(numSeedMessages, valueList.size(), String.format("Expected %d values in %s.", numSeedMessages, TOPIC2));
            assertEquals(valueList.size(), valueSet.size(), String.format("Expected %d unique messages in %s.", valueList.size(), TOPIC2));
        }
    }

    // Seeds the given topic with records with keys and values in the range [0..numRecords)
    static void seedTopicWithNumberedRecords(int numRecords, ClusterInstance instance) {
        var props = new Properties();
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, instance.bootstrapServers());
        try (var producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            for (int i = 0; i < numRecords; i++) {
                producer.send(new ProducerRecord<>(TOPIC1, String.valueOf(i).getBytes(), String.valueOf(i).getBytes()));
            }
            producer.flush();
        }
    }

    static void resetToCommittedPositions(Consumer<byte[], byte[]> consumer) {
        consumer.committed(consumer.assignment()).forEach((topicPartition, offsetAndMetadata) -> {
            if (offsetAndMetadata != null) {
                consumer.seek(topicPartition, offsetAndMetadata.offset());
            } else {
                consumer.seekToBeginning(Collections.singletonList(topicPartition));
            }
        });
    }

    static Map<TopicPartition, OffsetAndMetadata> consumerPositions(Consumer<byte[], byte[]> consumer) {
        HashMap<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        consumer.assignment().forEach(
                topicPartition -> offsetsToCommit.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)))
        );
        return offsetsToCommit;
    }

    static void testTimeout(
            ClusterInstance cluster,
            boolean needInitAndSendMsg,
            java.util.function.Consumer<KafkaProducer<byte[], byte[]>> timeoutProcess
    ) {
        var producer = createTransactionalProducer(cluster, "transactionProducer",
                2000, 3000, 4000, 1000);
        if (needInitAndSendMsg) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(TOPIC1, 0, "foo".getBytes(), "bar".getBytes()));
        }
        cluster.brokers().forEach((__, broker) -> broker.shutdown());
        assertThrows(TimeoutException.class, () -> timeoutProcess.accept(producer));
        producer.close(Duration.ZERO);
    }

    static List<ConsumerRecord<byte[], byte[]>> consumeRecordsFor(Consumer<byte[], byte[]> consumer) throws InterruptedException {
        var duration = 1000;
        var startTime = System.currentTimeMillis();
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(50)).forEach(records::add);
            return System.currentTimeMillis() - startTime > duration;
        }, "The timeout $duration was greater than the maximum wait time.");
        return records;
    }

    static void sendTransactionalMessagesWithValueRange(
            KafkaProducer<byte[], byte[]> producer,
            String topic,
            int start,
            int end,
            boolean willBeCommitted) {
        for (int i = start; i < end; i++) {
            producer.send(producerRecordWithExpectedTransactionStatus(topic, null, Integer.toString(i), Integer.toString(i), willBeCommitted));
        }
        producer.flush();
    }

    static int waitUntilLeaderIsKnown(ClusterInstance cluster, TopicPartition tp) throws InterruptedException {
        AtomicReference<Integer> leaderIfExists = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            Optional<Integer> leader = cluster.brokers().values()
                    .stream()
                    .filter(broker -> broker.replicaManager()
                            .onlinePartition(tp)
                            .exists(partition -> partition.leaderIdIfLocal().isDefined())
                    )
                    .map(broker -> broker.config().brokerId())
                    .findFirst();
            leaderIfExists.set(leader.orElse(null));
            return leader.isPresent();
        }, String.format("Partition %s leaders not made yet after %d ms", tp, 15000));
        return leaderIfExists.get();
    }

    static void restartDeadBrokers(ClusterInstance cluster) {
        cluster.brokers().forEach((__, broker) -> {
            if (broker.isShutdown()) {
                broker.startup();
            }
        });
    }

    static void testFailureToFenceEpoch(ClusterInstance cluster, boolean isTV2Enabled, Map<String, String> topicConfig) throws Exception {
        createTopics(cluster, topicConfig);
        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createTransactionalProducer(cluster, "transactional-producer", 2000, 1000, 4000, 1000);
             var producer3 = createTransactionalProducer(cluster, "transactional", 2000, 5000, 4000, 1000)
        ) {
            producer1.initTransactions();
            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 0, "4", "4", true));
            producer1.commitTransaction();

            var partitionLeader = waitUntilLeaderIsKnown(cluster, new TopicPartition(TOPIC1, 0));
            var broker = cluster.brokers().get(partitionLeader);
            var activeProducersIter = broker.logManager()
                    .getLog(new TopicPartition(TOPIC1, 0), false)
                    .get()
                    .producerStateManager()
                    .activeProducers()
                    .entrySet()
                    .iterator();
            assertTrue(activeProducersIter.hasNext());
            var producerStateEntry = activeProducersIter.next().getValue();
            var producerId = producerStateEntry.producerId();
            var initialProducerEpoch = producerStateEntry.producerEpoch();

            // Kill two brokers to bring the transaction log under min-ISR
            cluster.brokers().get(0).shutdown();
            cluster.brokers().get(1).shutdown();

            assertThrows(TimeoutException.class, producer2::initTransactions);

            try {
                producer2.initTransactions();
            } catch (TimeoutException e) {
                // good!
            } catch (Exception e) {
                throw new AssertionError("Got an unexpected exception from initTransactions", e);
            } finally {
                producer2.close();
            }

            restartDeadBrokers(cluster);

            // Because the epoch was bumped in memory, attempting to begin a transaction with producer 1 should fail
            try {
                producer1.beginTransaction();
            } catch (ProducerFencedException e) {
                // good!
            } catch (Exception e) {
                throw new AssertionError("Got an unexpected exception from commitTransaction", e);
            } finally {
                producer1.close();
            }

            producer3.initTransactions();
            producer3.beginTransaction();
            producer3.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 0, "4", "4", true));
            producer3.commitTransaction();

            if (isTV2Enabled) {
                // Wait until the producer epoch has been updated on the broker.
                TestUtils.waitForCondition(() -> {
                    ProducerStateEntry producerStateEntry1 = broker.logManager()
                            .getLog(new TopicPartition(TOPIC1, 0), false)
                            .get()
                            .producerStateManager()
                            .activeProducers()
                            .get(producerId);
                    return producerStateEntry1 != null && producerStateEntry1.producerEpoch() == initialProducerEpoch + 1;
                }, "Timed out waiting for producer epoch to be incremented after second commit");
            } else {
                // Check that the epoch only increased by 1
                producerStateEntry = broker.logManager()
                        .getLog(new TopicPartition(TOPIC1, 0), false)
                        .get()
                        .producerStateManager()
                        .activeProducers()
                        .get(producerId);
                assertNotNull(producerStateEntry);
                assertEquals(initialProducerEpoch + 1, producerStateEntry.producerEpoch());
            }
        }
    }

    static void testBasicTransactions(
            ClusterInstance cluster, 
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException {
        createTopics(cluster, topicConfig);

        try (var producer = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster, groupProtocol);
             var unCommittedConsumer = createReadUncommittedConsumer(cluster, groupProtocol)
        ) {
            var t1p1 = new TopicPartition(TOPIC1, 1);
            var t2p2 = new TopicPartition(TOPIC2, 2);

            producer.initTransactions();
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, 2, "2", "2", false));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 1, "4", "4", false));
            producer.flush();

            // Since we haven't committed/aborted any records, the last stable offset is still 0,
            // no segments should be offloaded to remote storage
            verifyLogStartOffsets(cluster, Map.of(t1p1, 0, t2p2, 0));
            producer.abortTransaction();

            // We've sent 1 record + 1 abort mark = 2 (segments) to each topic partition,
            // so 1 segment should be offloaded, the local log start offset should be 1
            // And log start offset is still 0
            verifyLogStartOffsets(cluster, Map.of(t1p1, 0, t2p2, 0));

            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, 1, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, 2, "3", "3", true));

            // Before records are committed, these records won't be offloaded.
            verifyLogStartOffsets(cluster, Map.of(t1p1, 0, t2p2, 0));

            producer.commitTransaction();

            // We've sent 2 records + 1 abort mark + 1 commit mark = 4 (segments) to each topic partition,
            // so 3 segments should be offloaded, the local log start offset should be 3
            // And log start offset is still 0
            verifyLogStartOffsets(cluster, Map.of(t1p1, 0, t2p2, 0));

            consumer.subscribe(List.of(t1p1.topic(), t2p2.topic()));
            unCommittedConsumer.subscribe(List.of(t1p1.topic(), t2p2.topic()));

            consumeRecords(consumer, 2).forEach(TransactionTestUtils::assertCommittedAndGetValue);

            var allRecords = consumeRecords(unCommittedConsumer, 4);
            var expectedValues = Set.of("1", "2", "3", "4");
            allRecords.forEach(record -> assertTrue(expectedValues.contains(recordValueAsString(record))));
        }
    }

    static void testReadCommittedConsumerShouldNotSeeUndecidedData(
            ClusterInstance cluster,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException {
        createTopics(cluster, topicConfig);

        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createTransactionalProducer(cluster, "other", 2000, 2000, 4000, 1000);
             var readCommittedConsumer = createDefaultReadCommittedConsumer(cluster, groupProtocol);
             var readUncommittedConsumer = createReadUncommittedConsumer(cluster, groupProtocol)
        ) {

            producer1.initTransactions();
            producer2.initTransactions();
            producer1.beginTransaction();
            producer2.beginTransaction();

            var latestVisibleTimestamp = System.currentTimeMillis();
            producer2.send(new ProducerRecord<>(TOPIC1, 0, latestVisibleTimestamp, "x".getBytes(UTF_8), "1".getBytes(UTF_8)));
            producer2.send(new ProducerRecord<>(TOPIC2, 0, latestVisibleTimestamp, "x".getBytes(UTF_8), "1".getBytes(UTF_8)));
            producer2.flush();

            var latestWrittenTimestamp = latestVisibleTimestamp + 1;
            producer1.send(new ProducerRecord<>(TOPIC1, 0, latestWrittenTimestamp, "a".getBytes(UTF_8), "1".getBytes(UTF_8)));
            producer1.send(new ProducerRecord<>(TOPIC1, 0, latestWrittenTimestamp, "b".getBytes(UTF_8), "2".getBytes(UTF_8)));
            producer1.send(new ProducerRecord<>(TOPIC2, 0, latestWrittenTimestamp, "c".getBytes(UTF_8), "3".getBytes(UTF_8)));
            producer1.send(new ProducerRecord<>(TOPIC2, 0, latestWrittenTimestamp, "d".getBytes(UTF_8), "4".getBytes(UTF_8)));
            producer1.flush();

            producer2.send(new ProducerRecord<>(TOPIC1, 0, latestWrittenTimestamp, "x".getBytes(UTF_8), "2".getBytes(UTF_8)));
            producer2.send(new ProducerRecord<>(TOPIC2, 0, latestWrittenTimestamp, "x".getBytes(UTF_8), "2".getBytes(UTF_8)));
            producer2.commitTransaction();

            var tp1 = new TopicPartition(TOPIC1, 0);
            var tp2 = new TopicPartition(TOPIC2, 0);
            readUncommittedConsumer.assign(Set.of(tp1, tp2));
            consumeRecords(readUncommittedConsumer, 8);

            var readUncommittedOffsetsForTimes =
                    readUncommittedConsumer.offsetsForTimes(Map.of(tp1, latestWrittenTimestamp, tp2, latestWrittenTimestamp));
            assertEquals(2, readUncommittedOffsetsForTimes.size());
            assertEquals(latestWrittenTimestamp, readUncommittedOffsetsForTimes.get(tp1).timestamp());
            assertEquals(latestWrittenTimestamp, readUncommittedOffsetsForTimes.get(tp2).timestamp());
            readUncommittedConsumer.unsubscribe();

            // we should only see the first two records which come before the undecided second transaction
            readCommittedConsumer.assign(Set.of(tp1, tp2));
            consumeRecords(readCommittedConsumer, 2).forEach(record -> {
                assertEquals("x", new String(record.key()));
                assertEquals("1", new String(record.value()));
            });

            // even if we seek to the end, we should not be able to see the undecided data
            assertEquals(2, readCommittedConsumer.assignment().size());
            readCommittedConsumer.seekToEnd(readCommittedConsumer.assignment());
            readCommittedConsumer.assignment().forEach(tp -> assertEquals(1L, readCommittedConsumer.position(tp)));

            // undecided timestamps should not be searchable either
            var readCommittedOffsetsForTimes =
                    readCommittedConsumer.offsetsForTimes(Map.of(tp1, latestWrittenTimestamp, tp2, latestWrittenTimestamp));
            assertNull(readCommittedOffsetsForTimes.get(tp1));
            assertNull(readCommittedOffsetsForTimes.get(tp2));
        }
    }

    static void testDelayedFetchIncludesAbortedTransaction(
            ClusterInstance cluster,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException {
        createTopics(cluster, topicConfig);
        // ensure that the consumer's fetch will sit in purgatory
        var consumerProps = new Properties();
        consumerProps.put(FETCH_MIN_BYTES_CONFIG, "100000");
        consumerProps.put(FETCH_MAX_WAIT_MS_CONFIG, "100");

        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createTransactionalProducer(cluster, "other", 2000, 2000, 4000, 1000);
             var readCommittedConsumer = createReadCommittedConsumer(cluster, "group", 500, groupProtocol, consumerProps)
        ) {
            var t1p0 = new TopicPartition(TOPIC1, 0);

            producer1.initTransactions();
            producer2.initTransactions();

            producer1.beginTransaction();
            producer2.beginTransaction();
            producer2.send(new ProducerRecord<>(TOPIC1, 0, "x".getBytes(), "1".getBytes()));
            producer2.flush();

            producer1.send(new ProducerRecord<>(TOPIC1, 0, "y".getBytes(), "1".getBytes()));
            producer1.send(new ProducerRecord<>(TOPIC1, 0, "y".getBytes(), "2".getBytes()));
            producer1.flush();

            producer2.send(new ProducerRecord<>(TOPIC1, 0, "x".getBytes(), "2".getBytes()));
            producer2.flush();

            // Since we haven't committed/aborted any records, the last stable offset is still 0,
            // no segments should be offloaded to remote storage
            verifyLogStartOffsets(cluster, Map.of(t1p0, 0));

            producer1.abortTransaction();
            producer2.commitTransaction();

            // We've sent 4 records + 1 abort mark + 1 commit mark = 6 (segments),
            // so 5 segments should be offloaded, the local log start offset should be 5
            // And log start offset is still 0
            verifyLogStartOffsets(cluster, Map.of(t1p0, 0));

            readCommittedConsumer.assign(Set.of(t1p0));
            var records = consumeRecords(readCommittedConsumer, 2);
            assertEquals(2, records.size());

            var first = records.get(0);
            assertEquals("x", new String(first.key()));
            assertEquals("1", new String(first.value()));
            assertEquals(0L, first.offset());

            var second = records.get(1);
            assertEquals("x", new String(second.key()));
            assertEquals("2", new String(second.value()));
            assertEquals(3L, second.offset());
        }
    }

    static void testFencingOnCommit(
            ClusterInstance cluster,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException {
        createTopics(cluster, topicConfig);
        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster, groupProtocol)
        ) {

            consumer.subscribe(List.of(TOPIC1, TOPIC2));

            producer1.initTransactions();

            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));

            producer2.initTransactions();  // ok, will abort the open transaction.
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true));
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true));

            assertThrows(ProducerFencedException.class, producer1::commitTransaction);

            producer2.commitTransaction();  // ok

            consumeRecords(consumer, 2).forEach(TransactionTestUtils::assertCommittedAndGetValue);
        }
    }

    static void testFencingOnSendOffsets(
            ClusterInstance cluster,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException {
        createTopics(cluster, topicConfig);
        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster, groupProtocol)
        ) {

            consumer.subscribe(List.of(TOPIC1, TOPIC2));

            producer1.initTransactions();

            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));
            producer1.flush();

            producer2.initTransactions();  // ok, will abort the open transaction.
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true));
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true));

            assertThrows(ProducerFencedException.class,
                    () -> producer1.sendOffsetsToTransaction(Map.of(
                                    new TopicPartition("foobartopic", 0), new OffsetAndMetadata(110L)
                            ), new ConsumerGroupMetadata("foobarGroup")
                    )
            );
            producer2.commitTransaction();  // ok
            consumeRecords(consumer, 2).forEach(TransactionTestUtils::assertCommittedAndGetValue);
        }
    }

    static void testOffsetMetadataInSendOffsetsToTransaction(
            ClusterInstance cluster,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException {
        createTopics(cluster, topicConfig);
        var tp = new TopicPartition(TOPIC1, 0);
        var groupId = "group";

        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createDefaultTransactionalProducer(cluster);
             var consumer = createReadCommittedConsumer(cluster, groupId, 100, groupProtocol, new Properties())
        ) {

            consumer.subscribe(List.of(TOPIC1));

            producer1.initTransactions();
            producer1.beginTransaction();

            var offsetAndMetadata = new OffsetAndMetadata(110L, Optional.of(15), "some metadata");
            producer1.sendOffsetsToTransaction(Map.of(tp, offsetAndMetadata), new ConsumerGroupMetadata(groupId));
            producer1.commitTransaction();  // ok

            // The call to commit the transaction may return before all markers are visible, so we initialize a second
            // producer to ensure the transaction completes and the committed offsets are visible.
            producer2.initTransactions();

            waitForCondition(() -> offsetAndMetadata.equals(consumer.committed(Set.of(tp)).get(tp)), "cannot read committed offset");
        }
    }

    static void testFencingOnSend(
            ClusterInstance cluster,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException {
        createTopics(cluster, topicConfig);
        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster, groupProtocol)
        ) {
            consumer.subscribe(List.of(TOPIC1, TOPIC2));

            producer1.initTransactions();
            producer1.beginTransaction();

            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));

            producer2.initTransactions();  // ok, will abort the open transaction.
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true));
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true));

            try {
                producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "5", false)).get();
                fail("Should not be able to send messages from a fenced producer.");
            } catch (ProducerFencedException e) {
                producer1.close();
            } catch (ExecutionException e) {
                assertInstanceOf(InvalidProducerEpochException.class, e.getCause());
            } catch (Exception e) {
                throw new AssertionError("Got an unexpected exception from a fenced producer.", e);
            }

            producer2.commitTransaction(); // ok
            consumeRecords(consumer, 2).forEach(TransactionTestUtils::assertCommittedAndGetValue);
        }
    }

    static void testFencingOnAddPartitions(
            ClusterInstance cluster,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        createTopics(cluster, topicConfig);
        try (var producer1 = createDefaultTransactionalProducer(cluster);
             var producer2 = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster, groupProtocol)
        ) {
            consumer.subscribe(List.of(TOPIC1, TOPIC2));

            producer1.initTransactions();
            producer1.beginTransaction();
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false));
            producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "3", "3", false));
            producer1.abortTransaction();

            producer2.initTransactions();  // ok, will abort the open transaction.
            producer2.beginTransaction();
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "4", true))
                    .get(20, SECONDS);
            producer2.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "4", true))
                    .get(20, SECONDS);

            try {
                producer1.beginTransaction();
                producer1.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "5", false)).get();
                fail("Should not be able to send messages from a fenced producer.");
            } catch (ProducerFencedException __) {

            } catch (ExecutionException e) {
                assertInstanceOf(ProducerFencedException.class, e.getCause());
            } catch (Exception e) {
                throw new AssertionError("Got an unexpected exception from a fenced producer.", e);
            }

            producer2.commitTransaction(); // ok
            consumeRecords(consumer, 2).forEach(TransactionTestUtils::assertCommittedAndGetValue);
        }
    }

    static void testBumpTransactionalEpochWithTV2Enabled(
            ClusterInstance cluster,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException {
        createTopics(cluster, topicConfig);
        try (var producer = createTransactionalProducer(cluster, "transactionalProducer", 2000, 2000, 5000, 5000);
             var consumer = createDefaultReadCommittedConsumer(cluster, groupProtocol);
        ) {
            // Create a topic with RF=1 so that a single broker failure will render it unavailable
            var testTopic = "test-topic";
            createTopic(cluster, testTopic, NUM_PARTITIONS, 1, Map.of());
            var partitionLeader = waitUntilLeaderIsKnown(cluster, new TopicPartition(testTopic, 0));

            producer.initTransactions();
            // First transaction: commit
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "4", "4", true));
            producer.commitTransaction();

            var kafkaBroker = cluster.brokers().get(partitionLeader);
            // Get producerId and epoch after first commit
            var log = kafkaBroker.logManager()
                    .getLog(new TopicPartition(testTopic, 0), false)
                    .get();
            var producerStateManager = log.producerStateManager();
            var activeProducersIter = producerStateManager.activeProducers()
                    .entrySet().iterator();
            assertTrue(activeProducersIter.hasNext());
            var producerStateEntry = activeProducersIter.next().getValue();
            var producerId = producerStateEntry.producerId();
            var previousProducerEpoch = producerStateEntry.producerEpoch();

            // Second transaction: abort
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "2", false));

            kafkaBroker.shutdown(); // kill the partition leader to prevent the batch from being submitted
            var failedFuture = producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", false));
            Thread.sleep(6000); // Wait for the record to time out
            restartDeadBrokers(cluster);

            assertFutureThrows(failedFuture, TimeoutException.class);
            producer.abortTransaction();

            // Get producer epoch after abortTransaction and verify it has increased.
            producerStateEntry = kafkaBroker.logManager()
                    .getLog(new TopicPartition(testTopic, 0), false)
                    .get()
                    .producerStateManager()
                    .activeProducers().get(producerId);
            // Assert that producerStateEntry is not null
            assertNotNull(producerStateEntry, "Producer state entry should not be null after abortTransaction");

            var currentProducerEpoch = producerStateEntry.producerEpoch();
            assertTrue(currentProducerEpoch > previousProducerEpoch,
                    "Producer epoch after abortTransaction ($currentProducerEpoch) should be greater than after first commit ($previousProducerEpoch)"
            );

            // Update previousProducerEpoch
            var producerEpoch = producerStateEntry.producerEpoch();

            // Third transaction: commit
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "2", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "4", "4", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", true));
            producer.commitTransaction();

            TestUtils.waitForCondition(() -> {
                var logOption = kafkaBroker.logManager().getLog(new TopicPartition(testTopic, 0), false);
                return logOption.exists(l -> {
                    var producerStateEntry1 = l.producerStateManager().activeProducers().get(producerId);
                    return producerStateEntry1 != null && producerStateEntry1.producerEpoch() > producerEpoch;
                });
            }, String.format("Timed out waiting for producer epoch to be incremented for topic %s after second commit", testTopic));

            consumer.subscribe(List.of(TOPIC1, TOPIC2, testTopic));

            consumeRecords(consumer, 5).forEach(TransactionTestUtils::assertCommittedAndGetValue);
        }
    }

    static void testFencingOnTransactionExpiration(
            ClusterInstance cluster,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException, ExecutionException {
        createTopics(cluster, topicConfig);
        try (var producer = createTransactionalProducer(cluster, "expiringProducer", 100, 2000, 4000, 1000);
             var nonTransactionalConsumer = createReadUncommittedConsumer(cluster, groupProtocol);
             var transactionalConsumer = createDefaultReadCommittedConsumer(cluster, groupProtocol)
        ) {
            producer.initTransactions();
            producer.beginTransaction();

            // The first message and hence the first AddPartitions request should be successfully sent.
            var firstMessageResult = producer.send(
                    producerRecordWithExpectedTransactionStatus(TOPIC1, null, "1", "1", false)
            ).get();
            assertTrue(firstMessageResult.hasOffset());

            // Wait for the expiration cycle to kick in.
            Thread.sleep(600);

            // Now that the transaction has expired, the second send should fail with a ProducerFencedException.
            var exception = assertThrows(ExecutionException.class,
                    () -> producer.send(producerRecordWithExpectedTransactionStatus(
                            TOPIC1, null, "2", "2", false)).get());
            assertInstanceOf(ProducerFencedException.class, exception.getCause());

            // Verify that the first message was aborted and the second one was never written at all.
            nonTransactionalConsumer.subscribe(List.of(TOPIC1));

            // Attempt to consume the one written record. We should not see the second. The
            // assertion does not strictly guarantee that the record wasn't written, but the
            // data is small enough that had it been written, it would have been in the first fetch.
            var records = consumeRecords(nonTransactionalConsumer, 1);
            assertEquals(1, records.size());
            assertEquals("1", recordValueAsString(records.get(0)));

            transactionalConsumer.subscribe(List.of(TOPIC1));

            var transactionalRecords = consumeRecordsFor(transactionalConsumer);
            assertTrue(transactionalRecords.isEmpty());
        }
    }

    static void testMultipleMarkersOneLeader(
            ClusterInstance cluster,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException {
        createTopics(cluster, topicConfig);
        try (var firstProducer = createDefaultTransactionalProducer(cluster);
             var consumer = createDefaultReadCommittedConsumer(cluster, groupProtocol);
             var unCommittedConsumer = createReadUncommittedConsumer(cluster, groupProtocol)
        ) {
            var topicWith10Partitions = "largeTopic";
            var topicWith10PartitionsAndOneReplica = "largeTopicOneReplica";

            createTopic(cluster, topicWith10Partitions, 10, cluster.brokers().size(), topicConfig);
            createTopic(cluster, topicWith10PartitionsAndOneReplica, 10, 1, Map.of());

            firstProducer.initTransactions();

            firstProducer.beginTransaction();
            sendTransactionalMessagesWithValueRange(firstProducer, topicWith10Partitions, 0, 5000, false);
            sendTransactionalMessagesWithValueRange(firstProducer, topicWith10PartitionsAndOneReplica, 5000, 10000, false);
            firstProducer.abortTransaction();

            firstProducer.beginTransaction();
            sendTransactionalMessagesWithValueRange(firstProducer, topicWith10Partitions, 10000, 11000, true);
            firstProducer.commitTransaction();

            consumer.subscribe(List.of(topicWith10PartitionsAndOneReplica, topicWith10Partitions));
            unCommittedConsumer.subscribe(List.of(topicWith10PartitionsAndOneReplica, topicWith10Partitions));

            consumeRecords(consumer, 1000).forEach(TransactionTestUtils::assertCommittedAndGetValue);

            var allRecords = consumeRecords(unCommittedConsumer, 11000);
            var expectedValues = IntStream.range(0, 11000).mapToObj(Integer::toString).collect(Collectors.toSet());
            allRecords.forEach(record -> assertTrue(expectedValues.contains(recordValueAsString(record))));
        }
    }

    static void testBumpTransactionalEpochWithTV2Disabled(
            ClusterInstance cluster,
            GroupProtocol groupProtocol,
            Map<String, String> topicConfig
    ) throws InterruptedException {
        createTopics(cluster, topicConfig);
        try (var producer = createTransactionalProducer(cluster, "transactionalProducer", 2000, 2000,
                5000, 5000);
             var consumer = createDefaultReadCommittedConsumer(cluster, groupProtocol)
        ) {
            // Create a topic with RF=1 so that a single broker failure will render it unavailable
            var testTopic = "test-topic";
            createTopic(cluster, testTopic, NUM_PARTITIONS, 1, Map.of());
            var partitionLeader = waitUntilLeaderIsKnown(cluster, new TopicPartition(testTopic, 0));

            producer.initTransactions();
            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "4", "4", true));
            producer.commitTransaction();

            var kafkaBroker = cluster.brokers().get(partitionLeader);

            var activeProducersIter = kafkaBroker.logManager()
                    .getLog(new TopicPartition(testTopic, 0), false)
                    .get()
                    .producerStateManager().activeProducers().entrySet().iterator();
            assertTrue(activeProducersIter.hasNext());
            var producerStateEntry = activeProducersIter.next().getValue();
            var producerId = producerStateEntry.producerId();
            var initialProducerEpoch = producerStateEntry.producerEpoch();

            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "2", "2", false));

            kafkaBroker.shutdown(); // kill the partition leader to prevent the batch from being submitted
            var failedFuture = producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", false));
            Thread.sleep(600); // Wait for the record to time out
            restartDeadBrokers(cluster);

            assertFutureThrows(failedFuture, TimeoutException.class);
            producer.abortTransaction();

            producer.beginTransaction();
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC2, null, "2", "2", true));
            producer.send(producerRecordWithExpectedTransactionStatus(TOPIC1, null, "4", "4", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "1", "1", true));
            producer.send(producerRecordWithExpectedTransactionStatus(testTopic, 0, "3", "3", true));
            producer.commitTransaction();

            consumer.subscribe(List.of(TOPIC1, TOPIC2, testTopic));

            consumeRecords(consumer, 5).forEach(TransactionTestUtils::assertCommittedAndGetValue);

            // Producers can safely abort and continue after the last record of a transaction timing out, so it's possible to
            // get here without having bumped the epoch. If bumping the epoch is possible, the producer will attempt to, so
            // check there that the epoch has actually increased
            producerStateEntry = kafkaBroker.logManager()
                    .getLog(new TopicPartition(testTopic, 0), false)
                    .get().producerStateManager().activeProducers().get(producerId);
            assertNotNull(producerStateEntry);
            assertTrue(producerStateEntry.producerEpoch() > initialProducerEpoch);
        }
    }

    @FunctionalInterface
    interface OffsetHandler {
        void commit(Producer<byte[], byte[]> producer, String groupId, Consumer<byte[], byte[]> consumer);
    }

    private static List<ClusterConfig> generator(Properties prop, boolean isTV2Enabled) {
        return List.of(ClusterConfig.defaultBuilder()
                .setBrokers(3)
                .setTypes(Set.of(KRAFT, CO_KRAFT))
                .setServerProperties(serverConfig(prop))
                .setTags(List.of("kraftGroupCoordinator"))
                .setFeatures(isTV2Enabled ?
                        Map.of(TRANSACTION_VERSION, (short) 2) :
                        Map.of(TRANSACTION_VERSION, (short) 0)
                )
                .build());
    }

    private static Map<String, String> serverConfig(Properties overridingProps) {
        Map<String, String> prop = new HashMap<>();
        prop.put(AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false");
        prop.put(OFFSETS_TOPIC_PARTITIONS_CONFIG, "1");
        prop.put(TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, "3");
        prop.put(TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
        prop.put(TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, "2");
        prop.put(CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "true");
        prop.put(UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
        prop.put(AUTO_LEADER_REBALANCE_ENABLE_CONFIG, "false");
        prop.put(GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0");
        prop.put(TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, "200");
        overridingProps.forEach((k, v) -> prop.put(k.toString(), v.toString()));
        return prop;
    }

    static void maybeWaitForAtLeastOneSegmentUpload(
            ClusterInstance cluster, 
            Set<TopicPartition> topicPartitions,
            boolean isSkip
    ) {
        if (!isSkip) {
            topicPartitions.forEach(topicPartition -> {
                List<BrokerLocalStorage> localStorages = cluster.brokers().values().stream()
                        .map(b -> new BrokerLocalStorage(b.config().brokerId(), CollectionConverters.asJava(b.config().logDirs().toSet()), STORAGE_WAIT_TIMEOUT_SEC))
                        .collect(Collectors.toList());
                localStorages
                        .stream()
                        // Select brokers which are assigned a replica of the topic-partition
                        .filter(s -> isAssignedReplica(cluster, topicPartition, s.getBrokerId()))
                        // Filter out inactive brokers, which may still contain log segments we would expect
                        // to be deleted based on the retention configuration.
                        .filter(s -> isAlive(cluster, s.getBrokerId()))
                        .forEach(localStorage ->
                                // Wait until the brokers local storage have been cleared from the inactive log segments.
                                localStorage.waitForAtLeastEarliestLocalOffset(topicPartition, 1L));
            });
        }
    }

    private static boolean isAssignedReplica(
            ClusterInstance cluster,
            TopicPartition topicPartition,
            Integer replicaId
    ) {
        Optional<KafkaBroker> brokerOpt = cluster.brokers().values()
                .stream()
                .filter(b -> b.config().brokerId() == replicaId).findFirst();
        boolean isAssigned = false;
        if (brokerOpt.isPresent()) {
            HostedPartition hostedPartition = brokerOpt.get().replicaManager().getPartition(topicPartition);
            if (hostedPartition instanceof HostedPartition.Online) {
                isAssigned = true;
            }
        }
        return isAssigned;
    }

    private static boolean isAlive(ClusterInstance cluster, Integer brokerId) {
        return cluster.brokers().values().stream().anyMatch(b -> b.config().brokerId() == brokerId);
    }

    private TransactionTestUtils() {
    }
}
