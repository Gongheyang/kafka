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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

public class ProspectiveState implements VotingState {
    private final int localId;
    private final int epoch;
    private final OptionalInt leaderId;
    private final Optional<Endpoints> leaderEndpoints;
    private final Optional<ReplicaKey> votedKey;
    private final VoterSet voters;
//    private final long electionTimeoutMs;
//    private final Timer electionTimer;
    private final Map<Integer, VoterState> preVoteStates = new HashMap<>();
    private final Optional<LogOffsetMetadata> highWatermark;
    private final int electionTimeoutMs;
    private final Timer electionTimer;
    private final Logger log;

    /**
     * The lifetime of a prospective state is the following.
     *
     * 1. Once started, it will keep record of the received votes and continue to fetch from bootstrap voters.
     * 2. If it receives a fetch response denoting a leader with a higher epoch, it will transition to follower state.
     * 3. If majority votes granted, it will transition to leader state.
     * 4. If majority votes rejected or election times out, it will enter a backing off phase;
     *     after the backoff phase completes, it will send out another round of PreVote requests.
     */
    private boolean isBackingOff;

    public ProspectiveState(
        Time time,
        int localId,
        int epoch,
        OptionalInt leaderId,
        Optional<Endpoints> leaderEndpoints,
        Optional<ReplicaKey> votedKey,
        VoterSet voters,
        Optional<LogOffsetMetadata> highWatermark,
        int electionTimeoutMs,
        LogContext logContext
    ) {
        this.localId = localId;
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.leaderEndpoints = leaderEndpoints;
        this.votedKey = votedKey;
        this.voters = voters;
        this.highWatermark = highWatermark;
        this.isBackingOff = false;
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionTimer = time.timer(electionTimeoutMs);
        this.log = logContext.logger(ProspectiveState.class);

        for (ReplicaKey voter : voters.voterKeys()) {
            preVoteStates.put(voter.id(), new VoterState(voter));
        }
        preVoteStates.get(localId).setState(VoterState.State.GRANTED);
    }

    public int localId() {
        return localId;
    }

    public Optional<ReplicaKey> votedKey() {
        return votedKey;
    }

    @Override
    public Map<Integer, VoterState> voteStates() {
        return preVoteStates;
    }

    @Override
    public boolean isBackingOff() {
        return isBackingOff;
    }

    @Override
    public int retries() {
        return 1;
    }

    /**
     * Record a granted vote from one of the voters.
     *
     * @param remoteNodeId The id of the voter
     * @return true if the voter had not been previously recorded
     * @throws IllegalArgumentException if the remote node is not a voter
     */
    public boolean recordGrantedVote(int remoteNodeId) {
        VoterState voterState = preVoteStates.get(remoteNodeId);
        if (voterState == null) {
            throw new IllegalArgumentException("Attempt to grant vote to non-voter " + remoteNodeId);
        }

        boolean recorded = voterState.state().equals(VoterState.State.UNRECORDED);
        voterState.setState(VoterState.State.GRANTED);

        return recorded;
    }

    /**
     * Record a rejected vote from one of the voters.
     *
     * @param remoteNodeId The id of the voter
     * @return true if the rejected vote had not been previously recorded
     * @throws IllegalArgumentException if the remote node is not a voter
     */
    public boolean recordRejectedVote(int remoteNodeId) {
        VoterState voterState = preVoteStates.get(remoteNodeId);
        if (voterState == null) {
            throw new IllegalArgumentException("Attempt to reject vote to non-voter " + remoteNodeId);
        }
        if (remoteNodeId == localId) {
            throw new IllegalStateException("Attempted to reject vote from ourselves");
        }

        boolean recorded = voterState.state().equals(VoterState.State.UNRECORDED);
        voterState.setState(VoterState.State.REJECTED);

        return recorded;
    }

    /**
     * Record the current election has failed since we've either received sufficient rejecting voters or election timed out
     */
    public void startBackingOff(long currentTimeMs, long backoffDurationMs) {
        this.isBackingOff = true;
    }

    @Override
    public boolean canGrantVote(ReplicaKey replicaKey, boolean isLogUpToDate) {
        if (votedKey.isPresent()) {
            ReplicaKey votedReplicaKey = votedKey.get();
            if (votedReplicaKey.id() == replicaKey.id()) {
                return votedReplicaKey.directoryId().isEmpty() || votedReplicaKey.directoryId().equals(replicaKey.directoryId());
            }
            log.debug(
                "Rejecting Vote request from candidate ({}), already have voted for another " +
                    "candidate ({}) in epoch {}",
                replicaKey,
                votedKey,
                epoch
            );
            return false;
        } else if (!isLogUpToDate) {
            log.debug(
                "Rejecting Vote request from candidate ({}) since replica epoch/offset is not up to date with us",
                replicaKey
            );
        }

        return isLogUpToDate;
    }

    @Override
    public boolean canGrantPreVote(ReplicaKey replicaKey, boolean isLogUpToDate) {
        if (!isLogUpToDate) {
            log.debug(
                "Rejecting PreVote request from prospective ({}) since prospective epoch/offset is not up to date with us",
                replicaKey
            );
        }

        return isLogUpToDate;
    }

    @Override
    public boolean hasElectionTimeoutExpired(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.isExpired();
    }

    @Override
    public long remainingElectionTimeMs(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.remainingMs();
    }

    @Override
    public ElectionState election() {
        if (votedKey.isPresent()) {
            return ElectionState.withVotedCandidate(epoch, votedKey().get(), voters.voterIds());
        } else if (leaderId.isPresent()) {
            return ElectionState.withElectedLeader(epoch, leaderId.getAsInt(), voters.voterIds());
        } else {
            return ElectionState.withUnknownLeader(epoch, voters.voterIds());
        }
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public Endpoints leaderEndpoints() {
        return leaderEndpoints.orElse(Endpoints.empty());
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public String toString() {
        return String.format(
            "ProspectiveState(epoch=%d, votedKey=%s, voters=%s, highWatermark=%s)",
            epoch,
            votedKey,
            voters,
            highWatermark
        );
    }

    @Override
    public String name() {
        return "Prospective";
    }

    @Override
    public void close() {}
}
