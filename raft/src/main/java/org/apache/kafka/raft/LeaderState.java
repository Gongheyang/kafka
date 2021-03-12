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
package org.apache.kafka.raft;

import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * In the context of LeaderState, an acknowledged voter means one who has acknowledged the current leader by either
 * responding to a `BeginQuorumEpoch` request from the leader or by beginning to send `Fetch` requests.
 * More specifically, the set of unacknowledged voters are targets for BeginQuorumEpoch requests from the leader until
 * they acknowledge the leader.
 */
public class LeaderState implements EpochState {
    static final long OBSERVER_SESSION_TIMEOUT_MS = 300_000L;

    private final int localId;
    private final int epoch;
    private final long epochStartOffset;

    private Optional<LogOffsetMetadata> highWatermark;
    private final Map<Integer, VoterState> voterReplicaStates = new HashMap<>();
    private final Map<Integer, ReplicaState> observerReplicaStates = new HashMap<>();
    private final Set<Integer> grantingVoters = new HashSet<>();
    private final Logger log;

    protected LeaderState(
        int localId,
        int epoch,
        long epochStartOffset,
        Set<Integer> voters,
        Set<Integer> grantingVoters,
        LogContext logContext
    ) {
        this.localId = localId;
        this.epoch = epoch;
        this.epochStartOffset = epochStartOffset;
        this.highWatermark = Optional.empty();

        for (int voterId : voters) {
            boolean hasAcknowledgedLeader = voterId == localId;
            this.voterReplicaStates.put(voterId, new VoterState(voterId, hasAcknowledgedLeader));
        }
        this.grantingVoters.addAll(grantingVoters);
        this.log = logContext.logger(LeaderState.class);
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withElectedLeader(epoch, localId, voterReplicaStates.keySet());
    }

    @Override
    public int epoch() {
        return epoch;
    }

    public Set<Integer> followers() {
        return voterReplicaStates.keySet().stream().filter(id -> id != localId).collect(Collectors.toSet());
    }

    public Set<Integer> grantingVoters() {
        return this.grantingVoters;
    }

    public int localId() {
        return localId;
    }

    public Set<Integer> nonAcknowledgingVoters() {
        Set<Integer> nonAcknowledging = new HashSet<>();
        for (VoterState state : voterReplicaStates.values()) {
            if (!state.hasAcknowledgedLeader)
                nonAcknowledging.add(state.nodeId);
        }
        return nonAcknowledging;
    }

    private boolean updateHighWatermark() {
        // Find the largest offset which is replicated to a majority of replicas (the leader counts)
        List<VoterState> followersByDescendingFetchOffset = followersByDescendingFetchOffset();

        int indexOfHw = voterReplicaStates.size() / 2;
        Optional<LogOffsetMetadata> highWatermarkUpdateOpt = followersByDescendingFetchOffset.get(indexOfHw).endOffset;

        if (highWatermarkUpdateOpt.isPresent()) {
            // When a leader is first elected, it cannot know the high watermark of the previous
            // leader. In order to avoid exposing a non-monotonically increasing value, we have
            // to wait for followers to catch up to the start of the leader's epoch.
            LogOffsetMetadata highWatermarkUpdateMetadata = highWatermarkUpdateOpt.get();
            long highWatermarkUpdateOffset = highWatermarkUpdateMetadata.offset;

            if (highWatermarkUpdateOffset >= epochStartOffset) {
                if (highWatermark.isPresent()) {
                    LogOffsetMetadata currentHighWatermarkMetadata = highWatermark.get();
                    if (highWatermarkUpdateOffset > currentHighWatermarkMetadata.offset
                        || (highWatermarkUpdateOffset == currentHighWatermarkMetadata.offset &&
                            !highWatermarkUpdateMetadata.metadata.equals(currentHighWatermarkMetadata.metadata))) {
                        highWatermark = highWatermarkUpdateOpt;
                        return true;
                    } else if (highWatermarkUpdateOffset < currentHighWatermarkMetadata.offset) {
                        log.error("The latest computed high watermark {} is smaller than the current " +
                                "value {}, which suggests that one of the voters has lost committed data. " +
                                "Full voter replication state: {}", highWatermarkUpdateOffset,
                            currentHighWatermarkMetadata.offset, voterReplicaStates.values());
                        return false;
                    } else {
                        return false;
                    }
                } else {
                    highWatermark = highWatermarkUpdateOpt;
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Update the local replica state.
     *
     * See {@link #updateReplicaState(int, long, LogOffsetMetadata)}
     */
    public boolean updateLocalState(long fetchTimestamp, LogOffsetMetadata logOffsetMetadata) {
        return updateReplicaState(localId, fetchTimestamp, logOffsetMetadata);
    }

    /**
     * Update the replica state in terms of fetch time and log end offsets.
     *
     * @param replicaId replica id
     * @param fetchTimestamp fetch timestamp
     * @param logOffsetMetadata new log offset and metadata
     * @return true if the high watermark is updated too
     */
    public boolean updateReplicaState(int replicaId,
                                      long fetchTimestamp,
                                      LogOffsetMetadata logOffsetMetadata) {
        // Ignore fetches from negative replica id, as it indicates
        // the fetch is from non-replica. For example, a consumer.
        if (replicaId < 0) {
            return false;
        }

        ReplicaState state = getReplicaState(replicaId);
        state.updateFetchTimestamp(fetchTimestamp);
        return updateEndOffset(state, logOffsetMetadata);
    }

    public List<Integer> nonLeaderVotersByDescendingFetchOffset() {
        return followersByDescendingFetchOffset().stream()
            .filter(state -> state.nodeId != localId)
            .map(state -> state.nodeId)
            .collect(Collectors.toList());
    }

    private List<VoterState> followersByDescendingFetchOffset() {
        return new ArrayList<>(this.voterReplicaStates.values()).stream()
            .sorted()
            .collect(Collectors.toList());
    }

    private boolean updateEndOffset(ReplicaState state,
                                    LogOffsetMetadata endOffsetMetadata) {
        state.endOffset.ifPresent(currentEndOffset -> {
            if (currentEndOffset.offset > endOffsetMetadata.offset) {
                if (state.nodeId == localId) {
                    throw new IllegalStateException("Detected non-monotonic update of local " +
                        "end offset: " + currentEndOffset.offset + " -> " + endOffsetMetadata.offset);
                } else {
                    log.warn("Detected non-monotonic update of fetch offset from nodeId {}: {} -> {}",
                        state.nodeId, currentEndOffset.offset, endOffsetMetadata.offset);
                }
            }
        });

        state.endOffset = Optional.of(endOffsetMetadata);

        if (isVoter(state.nodeId)) {
            ((VoterState) state).hasAcknowledgedLeader = true;
            addAcknowledgementFrom(state.nodeId);
            return updateHighWatermark();
        }
        return false;
    }

    public void addAcknowledgementFrom(int remoteNodeId) {
        VoterState voterState = ensureValidVoter(remoteNodeId);
        voterState.hasAcknowledgedLeader = true;
    }

    private VoterState ensureValidVoter(int remoteNodeId) {
        VoterState state = voterReplicaStates.get(remoteNodeId);
        if (state == null)
            throw new IllegalArgumentException("Unexpected acknowledgement from non-voter " + remoteNodeId);
        return state;
    }

    public long epochStartOffset() {
        return epochStartOffset;
    }

    private ReplicaState getReplicaState(int remoteNodeId) {
        ReplicaState state = voterReplicaStates.get(remoteNodeId);
        if (state == null) {
            observerReplicaStates.putIfAbsent(remoteNodeId, new ObserverState(remoteNodeId));
            return observerReplicaStates.get(remoteNodeId);
        }
        return state;
    }

    Map<Integer, Long> getVoterEndOffsets() {
        return getReplicaEndOffsets(voterReplicaStates);
    }

    Map<Integer, Long> getObserverStates(final long currentTimeMs) {
        clearInactiveObservers(currentTimeMs);
        return getReplicaEndOffsets(observerReplicaStates);
    }

    private static <R extends ReplicaState> Map<Integer, Long> getReplicaEndOffsets(
        Map<Integer, R> replicaStates) {
        return replicaStates.entrySet().stream()
                   .collect(Collectors.toMap(Map.Entry::getKey,
                       e -> e.getValue().endOffset.map(
                           logOffsetMetadata -> logOffsetMetadata.offset).orElse(-1L))
                   );
    }

    private void clearInactiveObservers(final long currentTimeMs) {
        observerReplicaStates.entrySet().removeIf(
            integerReplicaStateEntry ->
                currentTimeMs - integerReplicaStateEntry.getValue().lastFetchTimestamp.orElse(-1)
                    >= OBSERVER_SESSION_TIMEOUT_MS);
    }

    private boolean isVoter(int remoteNodeId) {
        return voterReplicaStates.containsKey(remoteNodeId);
    }

    private static abstract class ReplicaState implements Comparable<ReplicaState> {
        final int nodeId;
        Optional<LogOffsetMetadata> endOffset;
        OptionalLong lastFetchTimestamp;

        public ReplicaState(int nodeId) {
            this.nodeId = nodeId;
            this.endOffset = Optional.empty();
            this.lastFetchTimestamp = OptionalLong.empty();
        }

        void updateFetchTimestamp(long currentFetchTimeMs) {
            // To be resilient to system time shifts we do not strictly
            // require the timestamp be monotonically increasing.
            lastFetchTimestamp = OptionalLong.of(Math.max(lastFetchTimestamp.orElse(-1L), currentFetchTimeMs));
        }

        @Override
        public int compareTo(ReplicaState that) {
            if (this.endOffset.equals(that.endOffset))
                return Integer.compare(this.nodeId, that.nodeId);
            else if (!this.endOffset.isPresent())
                return 1;
            else if (!that.endOffset.isPresent())
                return -1;
            else
                return Long.compare(that.endOffset.get().offset, this.endOffset.get().offset);
        }
    }

    private static class ObserverState extends ReplicaState {

        public ObserverState(int nodeId) {
            super(nodeId);
        }

        @Override
        public String toString() {
            return "Observer(" +
                "nodeId=" + nodeId +
                ", endOffset=" + endOffset +
                ", lastFetchTimestamp=" + lastFetchTimestamp +
                ')';
        }
    }

    private static class VoterState extends ReplicaState {
        boolean hasAcknowledgedLeader;

        public VoterState(int nodeId,
                          boolean hasAcknowledgedLeader) {
            super(nodeId);
            this.hasAcknowledgedLeader = hasAcknowledgedLeader;
        }

        @Override
        public String toString() {
            return "Voter(" +
                "nodeId=" + nodeId +
                ", endOffset=" + endOffset +
                ", lastFetchTimestamp=" + lastFetchTimestamp +
                ", hasAcknowledgedLeader=" + hasAcknowledgedLeader +
                ')';
        }
    }

    @Override
    public String toString() {
        return "Leader(" +
            "localId=" + localId +
            ", epoch=" + epoch +
            ", epochStartOffset=" + epochStartOffset +
            ')';
    }

    @Override
    public String name() {
        return "Leader";
    }

    @Override
    public void close() {}

}
