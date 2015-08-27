/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assign(List)} (manual assignment)
 * or with {@link #changePartitionAssignment(List)} (automatic assignment).
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seek(TopicPartition, long)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
 * This class also maintains a cache of the latest commit position for each of the assigned
 * partitions. This is updated through {@link #committed(TopicPartition, long)} and can be used
 * to set the initial fetch position (e.g. {@link Fetcher#resetOffset(TopicPartition)}.
 */
public class SubscriptionState {

    /* the list of topics the user has requested */
    private final Set<String> subscription;

    /* the list of partitions the user has requested */
    private final Set<TopicPartition> userAssignment;

    /* the list of partitions currently assigned */
    private final Map<TopicPartition, TopicPartitionState> assignment;

    /* do we need to request a partition assignment from the coordinator? */
    private boolean needsPartitionAssignment;

    /* do we need to request the latest committed offsets from the coordinator? */
    private boolean needsFetchCommittedOffsets;

    /* Default offset reset strategy */
    private final OffsetResetStrategy defaultResetStrategy;

    /* Listener to be invoked when assignment changes */
    private RebalanceListener listener;

    public SubscriptionState(OffsetResetStrategy defaultResetStrategy) {
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = new HashSet<>();
        this.userAssignment = new HashSet<>();
        this.assignment = new HashMap<>();
        this.needsPartitionAssignment = false;
        this.needsFetchCommittedOffsets = true; // initialize to true for the consumers to fetch offset upon starting up
    }

    public void subscribe(List<String> topics, RebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        if (!this.userAssignment.isEmpty())
            throw new IllegalStateException("Subscription to topics and partitions are mutually exclusive");

        this.listener = listener;

        if (!this.subscription.equals(new HashSet<>(topics))) {
            this.subscription.clear();
            this.subscription.addAll(topics);
            this.needsPartitionAssignment = true;

            // Remove any assigned partitions which are no longer subscribed to
            for (Iterator<TopicPartition> it = assignment.keySet().iterator(); it.hasNext(); ) {
                TopicPartition tp = it.next();
                if (!subscription.contains(tp.topic()))
                    it.remove();
            }
        }
    }

    public void needReassignment() {
        this.needsPartitionAssignment = true;
    }

    public void assign(List<TopicPartition> partitions) {
        if (!this.subscription.isEmpty())
            throw new IllegalStateException("Subscription to topics and partitions are mutually exclusive");

        this.userAssignment.clear();
        this.userAssignment.addAll(partitions);

        for (TopicPartition partition : partitions)
            if (!assignment.containsKey(partition))
                addAssignedPartition(partition);

        this.assignment.keySet().retainAll(this.userAssignment);
    }

    public void clearAssignment() {
        this.assignment.clear();
        this.needsPartitionAssignment = !subscription().isEmpty();
    }

    public Set<String> subscription() {
        return this.subscription;
    }

    public Long fetched(TopicPartition tp) {
        return assignedState(tp).fetched;
    }

    public void fetched(TopicPartition tp, long offset) {
        assignedState(tp).fetched(offset);
    }

    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.get(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    public void committed(TopicPartition tp, long offset) {
        assignedState(tp).committed(offset);
    }

    public Long committed(TopicPartition tp) {
        return assignedState(tp).committed;
    }

    public void needRefreshCommits() {
        this.needsFetchCommittedOffsets = true;
    }

    public boolean refreshCommitsNeeded() {
        return this.needsFetchCommittedOffsets;
    }

    public void commitsRefreshed() {
        this.needsFetchCommittedOffsets = false;
    }

    public void seek(TopicPartition tp, long offset) {
        assignedState(tp).seek(offset);
    }

    public Set<TopicPartition> assignedPartitions() {
        return this.assignment.keySet();
    }

    public Set<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> fetchable = new HashSet<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet()) {
            if (entry.getValue().isFetchable())
                fetchable.add(entry.getKey());
        }
        return fetchable;
    }

    public boolean partitionsAutoAssigned() {
        return !this.subscription.isEmpty();
    }

    public void consumed(TopicPartition tp, long offset) {
        assignedState(tp).consumed(offset);
    }

    public Long consumed(TopicPartition tp) {
        return assignedState(tp).consumed;
    }

    public Map<TopicPartition, Long> allConsumed() {
        Map<TopicPartition, Long> allConsumed = new HashMap<>();
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet()) {
            TopicPartitionState state = entry.getValue();
            if (state.hasValidPosition)
                allConsumed.put(entry.getKey(), state.consumed);
        }
        return allConsumed;
    }

    public void needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).awaitReset(offsetResetStrategy);
    }

    public void needOffsetReset(TopicPartition partition) {
        needOffsetReset(partition, defaultResetStrategy);
    }

    public boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset;
    }

    public OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy;
    }

    public boolean hasAllFetchPositions() {
        for (TopicPartitionState state : assignment.values())
            if (!state.hasValidPosition)
                return false;
        return true;
    }

    public Set<TopicPartition> missingFetchPositions() {
        Set<TopicPartition> missing = new HashSet<>(this.assignment.keySet());
        for (Map.Entry<TopicPartition, TopicPartitionState> entry : assignment.entrySet())
            if (!entry.getValue().hasValidPosition)
                missing.add(entry.getKey());
        return missing;
    }

    public boolean partitionAssignmentNeeded() {
        return this.needsPartitionAssignment;
    }

    public void changePartitionAssignment(List<TopicPartition> assignments) {
        for (TopicPartition tp : assignments)
            if (!this.subscription.contains(tp.topic()))
                throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic.");
        this.clearAssignment();
        for (TopicPartition tp: assignments)
            addAssignedPartition(tp);
        this.needsPartitionAssignment = false;
    }

    public boolean isAssigned(TopicPartition tp) {
        return assignment.containsKey(tp);
    }

    public boolean isPaused(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).paused;
    }

    public boolean isFetchable(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).isFetchable();
    }

    public void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    public void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    private void addAssignedPartition(TopicPartition tp) {
        this.assignment.put(tp, new TopicPartitionState());
    }

    public RebalanceListener listener() {
        return listener;
    }

    private static class TopicPartitionState {
        private Long consumed;   // offset exposed to the user
        private Long fetched;    // current fetch position
        private Long committed;  // last committed position

        private boolean hasValidPosition; // whether we have valid consumed and fetched positions
        private boolean paused;  // whether this partition has been paused by the user
        private boolean awaitingReset; // whether we are awaiting reset
        private OffsetResetStrategy resetStrategy;  // the reset strategy if awaitingReset is set

        public TopicPartitionState() {
            this.paused = false;
            this.consumed = null;
            this.fetched = null;
            this.committed = null;
            this.awaitingReset = false;
            this.hasValidPosition = false;
            this.resetStrategy = null;
        }

        private void awaitReset(OffsetResetStrategy strategy) {
            this.awaitingReset = true;
            this.resetStrategy = strategy;
            this.consumed = null;
            this.fetched = null;
            this.hasValidPosition = false;
        }

        private void seek(long offset) {
            this.consumed = offset;
            this.fetched = offset;
            this.awaitingReset = false;
            this.resetStrategy = null;
            this.hasValidPosition = true;
        }

        private void fetched(long offset) {
            if (!hasValidPosition)
                throw new IllegalStateException("Cannot update fetch position without valid consumed/fetched positions");
            this.fetched = offset;
        }

        private void consumed(long offset) {
            if (!hasValidPosition)
                throw new IllegalStateException("Cannot update consumed position without valid consumed/fetched positions");
            this.consumed = offset;
        }

        private void committed(Long offset) {
            this.committed = offset;
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition;
        }

    }

    public static RebalanceListener wrapListener(final Consumer<?, ?> consumer,
                                                 final ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("ConsumerRebalanceLister must not be null");

        return new RebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                listener.onPartitionsAssigned(consumer, partitions);
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                listener.onPartitionsRevoked(consumer, partitions);
            }

            @Override
            public ConsumerRebalanceListener underlying() {
                return listener;
            }
        };
    }

    /**
     * Wrapper around {@link ConsumerRebalanceListener} to get around the need to provide a reference
     * to the consumer in this class.
     */
    public static class RebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        }

        public ConsumerRebalanceListener underlying() {
            return null;
        }
    }


}