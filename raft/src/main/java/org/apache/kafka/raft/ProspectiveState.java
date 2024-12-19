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
import org.apache.kafka.raft.internals.EpochElection;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.OptionalInt;

public class ProspectiveState implements NomineeState {
    private final int localId;
    private final int epoch;
    private final OptionalInt leaderId;
    private final Optional<Endpoints> leaderEndpoints;
    private final Optional<ReplicaKey> votedKey;
    private final VoterSet voters;
    private final EpochElection epochElection;
    private final Optional<LogOffsetMetadata> highWatermark;
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
        this.electionTimer = time.timer(electionTimeoutMs);
        this.log = logContext.logger(ProspectiveState.class);

        this.epochElection = new EpochElection(voters.voterKeys());
        epochElection.recordVote(localId, true); //voterStates().get(localId).setState(State.GRANTED);
    }

    public int localId() {
        return localId;
    }

    public Optional<ReplicaKey> votedKey() {
        return votedKey;
    }

    @Override
    public EpochElection epochElection() {
        return epochElection;
    }

    @Override
    public boolean recordGrantedVote(int remoteNodeId) {
        return epochElection().recordVote(remoteNodeId, true);
    }

    @Override
    public boolean recordRejectedVote(int remoteNodeId) {
        if (remoteNodeId == localId) {
            throw new IllegalArgumentException("Attempted to reject vote from ourselves");
        }
        return epochElection().recordVote(remoteNodeId, false);
    }

    @Override
    public boolean canGrantVote(ReplicaKey replicaKey, boolean isLogUpToDate, boolean isPreVote) {
        if (isPreVote) {
            return canGrantPreVote(replicaKey, isLogUpToDate);
        }
        if (votedKey.isPresent()) {
            ReplicaKey votedReplicaKey = votedKey.get();
            if (votedReplicaKey.id() == replicaKey.id()) {
                return votedReplicaKey.directoryId().isEmpty() || votedReplicaKey.directoryId().equals(replicaKey.directoryId());
            }
            log.debug(
                "Rejecting Vote request (preVote=false) from candidate ({}), already have voted for another " +
                    "candidate ({}) in epoch {}",
                replicaKey,
                votedKey,
                epoch
            );
            return false;
        } else if (!isLogUpToDate) {
            log.debug(
                "Rejecting Vote request (preVote=false) from candidate ({}) since candidate's log is not up to date with us",
                replicaKey
            );
        }

        return isLogUpToDate;
    }

    private boolean canGrantPreVote(ReplicaKey replicaKey, boolean isLogUpToDate) {
        if (!isLogUpToDate) {
            log.debug(
                "Rejecting Vote request (preVote=true) from prospective ({}) since prospective's log is not up to date with us",
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
        return new ElectionState(epoch, leaderId, votedKey, voters.voterIds());
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
