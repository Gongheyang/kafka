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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrototypeAsyncConsumerTest {

    private PrototypeAsyncConsumer<?, ?> consumer;
    private Map<String, Object> consumerProps = new HashMap<>();

    private final Time time = new MockTime();
    private LogContext logContext;
    private SubscriptionState subscriptions;
    private EventHandler eventHandler;
    private Metrics metrics;
    private ClusterResourceListeners clusterResourceListeners;

    private String groupId = "group.id";
    private String clientId = "client-1";
    private ConsumerConfig config;

    @BeforeEach
    public void setup() {
        injectConsumerConfigs();
        this.config = new ConsumerConfig(consumerProps);
        this.eventHandler = mock(DefaultEventHandler.class);
        this.subscriptions = mock(SubscriptionState.class);
        this.logContext = new LogContext();
        this.metrics = new Metrics(time);
        this.clusterResourceListeners = new ClusterResourceListeners();
    }

    @AfterEach
    public void cleanup() {
        assertTrue(consumer.wakeupStateResetted());
        if (consumer != null) {
            consumer.close(Duration.ZERO);
        }
    }

    @Test
    public void testSuccessfulStartupShutdown() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertDoesNotThrow(() -> consumer.close());
    }

    @Test
    public void testInvalidGroupId() {
        this.groupId = null;
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertThrows(InvalidGroupIdException.class, () -> consumer.committed(new HashSet<>()));
    }

    @Test
    public void testCommitAsync_NullCallback() throws InterruptedException {
        WakeupableFuture<Void> future = new WakeupableFuture<>();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("my-topic", 0), new OffsetAndMetadata(100L));
        offsets.put(new TopicPartition("my-topic", 1), new OffsetAndMetadata(200L));

        consumer = spy(newConsumer(time, new StringDeserializer(), new StringDeserializer()));
        doReturn(future).when(consumer).commit(offsets);
        consumer.commitAsync(offsets, null);
        future.complete(null);
        TestUtils.waitForCondition(() -> future.isDone(),
                2000,
                "commit future should complete");

        assertFalse(future.isCompletedExceptionally());
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testCommitAsync_UserSuppliedCallback() {
        WakeupableFuture<Void> future = new WakeupableFuture<>();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("my-topic", 0), new OffsetAndMetadata(100L));
        offsets.put(new TopicPartition("my-topic", 1), new OffsetAndMetadata(200L));
        consumer = spy(newConsumer(time, new StringDeserializer(),
            new StringDeserializer()));
        doReturn(future).when(consumer).commit(offsets);
        OffsetCommitCallback customCallback = mock(OffsetCommitCallback.class);
        consumer.commitAsync(offsets, customCallback);
        future.complete(null);
        verify(customCallback).onComplete(offsets, null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCommitted() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        mockConstruction(OffsetFetchApplicationEvent.class, (mock, ctx) -> {
            when(mock.future()).thenReturn(new WakeupableFuture<>());
            when(mock.complete(any())).thenReturn(new HashMap<>());
        });
        Set<TopicPartition> mockTopicPartitions = mockTopicPartitionOffset().keySet();
        assertDoesNotThrow(() -> consumer.committed(mockTopicPartitions, Duration.ofMillis(1)));
        verify(eventHandler).add(ArgumentMatchers.isA(OffsetFetchApplicationEvent.class));
        assertTrue(consumer.wakeupStateResetted());
    }

    @Test
    public void testUnimplementedException() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertThrows(KafkaException.class, consumer::assignment, "not implemented exception");
    }

    @Test
    public void testWakeup_commitSync() {
        consumer = newConsumer(time, new StringDeserializer(),
            new StringDeserializer());
        doReturn(mockTopicPartitionOffset()).when(subscriptions).allConsumed();
        consumer.wakeup();
        assertThrows(WakeupException.class, () -> consumer.commitSync());
        assertTrue(consumer.wakeupStateResetted());
    }

    @Test
    public void testWakeup_committed() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        consumer.wakeup();
        Set<TopicPartition> mockTopicPartitions = mockTopicPartitionOffset().keySet();
        assertThrows(WakeupException.class, () -> consumer.committed(mockTopicPartitions));
        // empty topic list should return early
        assertDoesNotThrow(() -> consumer.committed(new HashSet<>()));
        assertTrue(consumer.wakeupStateResetted());
    }

    @Test
    public void testClosed() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        consumer.close();
        assertThrows(IllegalStateException.class, () -> consumer.committed(new HashSet<>()));
    }

    private HashMap<TopicPartition, OffsetAndMetadata> mockTopicPartitionOffset() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L));
        return topicPartitionOffsets;
    }

    private ConsumerMetadata createMetadata(SubscriptionState subscription) {
        return new ConsumerMetadata(0, Long.MAX_VALUE, false, false,
                subscription, new LogContext(), new ClusterResourceListeners());
    }

    private void injectConsumerConfigs() {
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        consumerProps.put(DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");
        consumerProps.put(RETRY_BACKOFF_MS_CONFIG, "100");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    private PrototypeAsyncConsumer<?, ?> newConsumer(final Time time,
                                                     final Deserializer<?> keyDeserializer,
                                                     final Deserializer<?> valueDeserializer) {
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());

        return new PrototypeAsyncConsumer<>(
            time,
            logContext,
            config,
            subscriptions,
            eventHandler,
            metrics,
            clusterResourceListeners,
            Optional.ofNullable(this.groupId),
            clientId,
            config.getInt(DEFAULT_API_TIMEOUT_MS_CONFIG));
    }
}

