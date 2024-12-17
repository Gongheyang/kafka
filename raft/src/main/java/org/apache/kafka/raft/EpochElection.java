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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EpochElection {
    private Map<Integer, VoterState> voterStates;

    EpochElection(Set<ReplicaKey> voters) {
        this.voterStates = voters.stream()
            .collect(Collectors.toMap(
                ReplicaKey::id,
                VoterState::new
            ));
    }

    VoterState getVoterStateOrThrow(int voterId) {
        VoterState voterState = voterStates.get(voterId);
        if (voterState == null) {
            throw new IllegalArgumentException("Attempt to access voter state of non-voter " + voterId);
        }
        return voterState;
    }

    boolean recordVote(int voterId, boolean isGranted) {
        boolean wasUnrecorded = false;
        VoterState voterState = getVoterStateOrThrow(voterId);
        if (voterState.state == VoterState.State.UNRECORDED) {
            wasUnrecorded = true;
        }
        if (isGranted) {
            voterState.setState(VoterState.State.GRANTED);
        } else {
            voterState.setState(VoterState.State.REJECTED);
        }
        return wasUnrecorded;
    }

    boolean isGrantedVoter(int voterId) {
        return getVoterStateOrThrow(voterId).state == VoterState.State.GRANTED;
    }

    boolean isRejectedVoter(int voterId) {
        return getVoterStateOrThrow(voterId).state == VoterState.State.REJECTED;
    }

    Set<Integer> voterIds() {
        return Collections.unmodifiableSet(voterStates.keySet());
    }

    Collection<VoterState> voterStates() {
        return Collections.unmodifiableCollection(voterStates.values());
    }

    /**
     * Check whether we have received enough votes to conclude the election and become leader.
     *
     * @return true if at least a majority of nodes have granted the vote
     */
    boolean isVoteGranted() {
        return numGranted() >= majoritySize();
    }

    /**
     * Check if we have received enough rejections that it is no longer possible to reach a
     * majority of grants.
     *
     * @return true if the vote is rejected, false if the vote is already or can still be granted
     */
    boolean isVoteRejected() {
        return numGranted() + numUnrecorded() < majoritySize();
    }

    /**
     * Get the set of voters which have not been counted as granted or rejected yet.
     *
     * @return The set of unrecorded voters
     */
    Set<ReplicaKey> unrecordedVoters() {
        return votersOfState(VoterState.State.UNRECORDED).collect(Collectors.toSet());
    }

    /**
     * Get the set of voters that have granted our vote requests.
     *
     * @return The set of granting voters, which should always contain the localId
     */
    Set<Integer> grantingVoters() {
        return votersOfState(VoterState.State.GRANTED).map(ReplicaKey::id).collect(Collectors.toSet());
    }

    /**
     * Get the set of voters that have rejected our candidacy.
     *
     * @return The set of rejecting voters
     */
    Set<Integer> rejectingVoters() {
        return votersOfState(VoterState.State.REJECTED).map(ReplicaKey::id).collect(Collectors.toSet());
    }

    private Stream<ReplicaKey> votersOfState(VoterState.State state) {
        return voterStates
            .values()
            .stream()
            .filter(voterState -> voterState.state().equals(state))
            .map(VoterState::replicaKey);
    }

    private long numGranted() {
        return votersOfState(VoterState.State.GRANTED).count();
    }

    private long numUnrecorded() {
        return votersOfState(VoterState.State.UNRECORDED).count();
    }

    private int majoritySize() {
        return voterStates.size() / 2 + 1;
    }

    private static final class VoterState {
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
