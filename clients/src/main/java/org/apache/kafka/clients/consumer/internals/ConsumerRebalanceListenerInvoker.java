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
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

/**
 * This class encapsulates the invocation of the callback methods defined in the {@link ConsumerRebalanceListener}
 * interface. When consumer group partition assignment changes, these methods are invoked. This class wraps those
 * callback calls with logging, optional {@link Sensor} updates, and latency recording.
 * <p>
 * It handles the invocation of partition assignment, revocation, and loss callbacks, ensuring that metrics are
 * recorded for the time taken to invoke each callback.
 */
public class ConsumerRebalanceListenerInvoker {
    private final Logger log;
    private final SubscriptionState subscriptions;
    private final Time time;
    private final RebalanceCallbackMetricsManager metricsManager;

    /**
     * Constructs a new {@link ConsumerRebalanceListenerInvoker} with the specified dependencies.
     *
     * @param logContext the log context to create a logger for this class
     * @param subscriptions the subscriptions state for the consumer
     * @param time the time utility for obtaining the current time in milliseconds
     * @param metricsManager the metrics manager to record callback latency
     */
    ConsumerRebalanceListenerInvoker(LogContext logContext,
                                     SubscriptionState subscriptions,
                                     Time time,
                                     RebalanceCallbackMetricsManager metricsManager) {
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.time = time;
        this.metricsManager = metricsManager;
    }

    /**
     * Invokes the onPartitionsAssigned callback method from the rebalance listener and logs the result.
     *
     * @param assignedPartitions the partitions assigned to the consumer
     * @return an exception if an error occurred, or null if no error
     */
    public Exception invokePartitionsAssigned(final SortedSet<TopicPartition> assignedPartitions) {
        return invokeRebalanceCallback("Adding newly assigned partitions", assignedPartitions,
                listener -> listener.onPartitionsAssigned(assignedPartitions),
                metricsManager::recordPartitionsAssignedLatency);
    }

    /**
     * Invokes the onPartitionsRevoked callback method from the rebalance listener and logs the result.
     *
     * @param revokedPartitions the partitions revoked from the consumer
     * @return an exception if an error occurred, or null if no error
     */
    public Exception invokePartitionsRevoked(final SortedSet<TopicPartition> revokedPartitions) {
        return invokeRebalanceCallback("Revoke previously assigned partitions", revokedPartitions,
                listener -> {
                    Set<TopicPartition> revokePausedPartitions = subscriptions.pausedPartitions();
                    revokePausedPartitions.retainAll(revokedPartitions);
                    if (!revokePausedPartitions.isEmpty()) {
                        log.info("The pause flag in partitions [{}] will be removed due to revocation.",
                                revokePausedPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));
                    }
                    listener.onPartitionsRevoked(revokedPartitions);
                },
                metricsManager::recordPartitionsRevokedLatency);
    }

    /**
     * Invokes the onPartitionsLost callback method from the rebalance listener and logs the result.
     *
     * @param lostPartitions the partitions lost from the consumer
     * @return an exception if an error occurred, or null if no error
     */
    public Exception invokePartitionsLost(final SortedSet<TopicPartition> lostPartitions) {
        return invokeRebalanceCallback("Lost previously assigned partitions", lostPartitions,
                listener -> {
                    Set<TopicPartition> lostPausedPartitions = subscriptions.pausedPartitions();
                    lostPausedPartitions.retainAll(lostPartitions);
                    if (!lostPausedPartitions.isEmpty()) {
                        log.info("The pause flag in partitions [{}] will be removed due to partition lost.",
                                lostPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));
                    }
                    listener.onPartitionsLost(lostPartitions);
                },
                metricsManager::recordPartitionsLostLatency);
    }

    /**
     * General method to invoke a rebalance callback method (assigned, revoked, or lost) and record latency.
     *
     * @param logMessage a message describing the action being taken (e.g., adding, revoking, losing)
     * @param partitions the partitions affected by the rebalance
     * @param invoker the specific method to invoke on the listener
     * @param latencyRecorder the method to record latency for the operation
     * @return an exception if an error occurred, or null if no error
     */
    private Exception invokeRebalanceCallback(String logMessage, SortedSet<TopicPartition> partitions,
                                              RebalanceListenerInvoker invoker,
                                              LatencyRecorder latencyRecorder) {
        log.info("{}: {}", logMessage, partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));

        Optional<ConsumerRebalanceListener> listener = subscriptions.rebalanceListener();
        if (listener.isPresent()) {
            try {
                final long startMs = time.milliseconds();
                invoker.invoke(listener.get());
                latencyRecorder.recordLatency(time.milliseconds() - startMs);
            } catch (WakeupException | InterruptException e) {
                throw e;
            } catch (Exception e) {
                log.error("User provided listener {} failed on invocation for partitions {}",
                        listener.get().getClass().getName(), partitions, e);
                return e;
            }
        }
        return null;
    }

    /**
     * Functional interface for invoking a rebalance callback method on the listener.
     */
    @FunctionalInterface
    private interface RebalanceListenerInvoker {
        void invoke(ConsumerRebalanceListener listener) throws Exception;
    }

    /**
     * Functional interface for recording latency during rebalance callback invocations.
     */
    @FunctionalInterface
    private interface LatencyRecorder {
        void recordLatency(long latency);
    }
}
