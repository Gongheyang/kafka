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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

interface VotingState extends EpochState {
    Map<Integer, VoterState> voteStates();

    default Stream<ReplicaKey> votersInState(VoterState.State state) {
        return voteStates()
            .values()
            .stream()
            .filter(voterState -> voterState.state().equals(state))
            .map(VoterState::replicaKey);
    }

    default int majoritySize() {
        return voteStates().size() / 2 + 1;
    }

    default long numGranted() {
        return votersInState(VoterState.State.GRANTED).count();
    }

    default long numUnrecorded() {
        return votersInState(VoterState.State.UNRECORDED).count();
    }

    /**
     * Check if the candidate is backing off for the next election
     */
    boolean isBackingOff();

    int retries();

    /**
     * Check whether we have received enough votes to conclude the election and become leader.
     *
     * @return true if at least a majority of nodes have granted the vote
     */
    default boolean isVoteGranted() {
        return numGranted() >= majoritySize();
    }

    /**
     * Check if we have received enough rejections that it is no longer possible to reach a
     * majority of grants.
     *
     * @return true if the vote is rejected, false if the vote is already or can still be granted
     */
    default boolean isVoteRejected() {
        return numGranted() + numUnrecorded() < majoritySize();
    }

    /**
     * Record a granted vote from one of the voters.
     *
     * @param remoteNodeId The id of the voter
     * @return true if the voter had not been previously recorded
     * @throws IllegalArgumentException
     */
    boolean recordGrantedVote(int remoteNodeId);

    /**
     * Record a rejected vote from one of the voters.
     *
     * @param remoteNodeId The id of the voter
     * @return true if the rejected vote had not been previously recorded
     * @throws IllegalArgumentException
     */
    boolean recordRejectedVote(int remoteNodeId);

    /**
     * Record the current election has failed since we've either received sufficient rejecting voters or election timed out
     */
    void startBackingOff(long currentTimeMs, long backoffDurationMs);

    boolean hasElectionTimeoutExpired(long currentTimeMs);

    long remainingElectionTimeMs(long currentTimeMs);

    /**
     * Get the set of voters which have not been counted as granted or rejected yet.
     *
     * @return The set of unrecorded voters
     */
    default Set<ReplicaKey> unrecordedVoters() {
        return votersInState(VoterState.State.UNRECORDED).collect(Collectors.toSet());
    }

    /**
     * Get the set of voters that have granted our vote requests.
     *
     * @return The set of granting voters, which should always contain the localId
     */
    default Set<Integer> grantingVoters() {
        return votersInState(VoterState.State.GRANTED).map(ReplicaKey::id).collect(Collectors.toSet());
    }

    /**
     * Get the set of voters that have rejected our candidacy.
     *
     * @return The set of rejecting voters
     */
    default Set<Integer> rejectingVoters() {
        return votersInState(VoterState.State.REJECTED).map(ReplicaKey::id).collect(Collectors.toSet());
    }

    final class VoterState {
        private final ReplicaKey replicaKey;
        private State state = State.UNRECORDED;

        VoterState(ReplicaKey replicaKey) {
            this.replicaKey = replicaKey;
        }

        public State state() {
            return state;
        }

        public void setState(State state) {
            this.state = state;
        }

        public ReplicaKey replicaKey() {
            return replicaKey;
        }

        enum State {
            UNRECORDED,
            GRANTED,
            REJECTED
        }
    }
}
