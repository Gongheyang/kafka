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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.Comparator;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConsumerRebalanceListenerInvokerTest {

    @Mock
    private LogContext logContext;
    @Mock
    private Logger log;
    @Mock
    private SubscriptionState subscriptions;
    @Mock
    private Time time;
    @Mock
    private RebalanceCallbackMetricsManager metricsManager;
    @Mock
    private ConsumerRebalanceListener listener;

    private ConsumerRebalanceListenerInvoker invoker;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        when(logContext.logger(any(Class.class))).thenReturn(log);

        invoker = new ConsumerRebalanceListenerInvoker(logContext, subscriptions, time, metricsManager);

        when(subscriptions.rebalanceListener()).thenReturn(Optional.of(listener));
    }

    @Test
    public void testInvokePartitionsAssigned() {
        SortedSet<TopicPartition> assignedPartitions = new TreeSet<>(Comparator.comparing(TopicPartition::toString));

        TopicPartition tp1 = new TopicPartition("testTopic", 0);
        assignedPartitions.add(tp1);

        Exception result = invoker.invokePartitionsAssigned(assignedPartitions);

        assertNull(result);
        verify(listener).onPartitionsAssigned(assignedPartitions);
        verify(metricsManager).recordPartitionsAssignedLatency(anyLong());
    }

    @Test
    public void testInvokePartitionsRevoked() {
        SortedSet<TopicPartition> revokedPartitions = new TreeSet<>(Comparator.comparing(TopicPartition::toString));

        TopicPartition tp1 = new TopicPartition("testTopic", 0);
        revokedPartitions.add(tp1);

        Exception result = invoker.invokePartitionsRevoked(revokedPartitions);

        assertNull(result);
        verify(listener).onPartitionsRevoked(revokedPartitions);
        verify(metricsManager).recordPartitionsRevokedLatency(anyLong());
    }

    @Test
    public void testInvokePartitionsLost() {
        SortedSet<TopicPartition> lostPartitions = new TreeSet<>(Comparator.comparing(TopicPartition::toString));

        TopicPartition tp1 = new TopicPartition("testTopic", 0);
        lostPartitions.add(tp1);

        Exception result = invoker.invokePartitionsLost(lostPartitions);

        assertNull(result);
        verify(listener).onPartitionsLost(lostPartitions);
        verify(metricsManager).recordPartitionsLostLatency(anyLong());
    }

    @Test
    public void testInvokeWithNoListener() {
        when(subscriptions.rebalanceListener()).thenReturn(Optional.empty());

        SortedSet<TopicPartition> assignedPartitions = new TreeSet<>(Comparator.comparing(TopicPartition::toString));
        TopicPartition tp1 = new TopicPartition("testTopic", 0);
        assignedPartitions.add(tp1);

        Exception result = invoker.invokePartitionsAssigned(assignedPartitions);

        assertNull(result);
        verify(listener, never()).onPartitionsAssigned(assignedPartitions);
    }
}
