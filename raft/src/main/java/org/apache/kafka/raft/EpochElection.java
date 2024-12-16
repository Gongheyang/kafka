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

public class EpochElection {
    private final int localId;
    private Map<Integer, VoterState> voterStates;

    EpochElection(int localId, Set<ReplicaKey> voters) {
        this.localId = localId;
        this.voterStates = voters.stream()
            .collect(Collectors.toMap(
                ReplicaKey::id,
                VoterState::new
            ));
    }

    Stream<ReplicaKey> voters(VoterState.State state) {
        return voterStates
            .values()
            .stream()
            .filter(voterState -> voterState.state().equals(state))
            .map(VoterState::replicaKey);
    }

    Map<Integer, VoterState> voterStates() {
        return voterStates;
    }

    int majoritySize() {
        return voterStates.size() / 2 + 1;
    }

    long numGranted() {
        return voters(VoterState.State.GRANTED).count();
    }

    long numUnrecorded() {
        return voters(VoterState.State.UNRECORDED).count();
    }

    /**
     * Check whether we have received enough votes to conclude the election and become leader.
     *
     * @return true if at least a majority of nodes have granted the vote
     */
    boolean isVoteGranted() {
        return numGranted() >= majoritySize();
    }

    boolean isVoteRejected() {
        return numGranted() + numUnrecorded() < majoritySize();
    }

    // this could be on prospective & candidate to check. this becomes grabbing the previous value and one generic record the vote (without checks)
    /**
     * Record a granted vote from one of the voters.
     *
     * @param remoteNodeId The id of the voter
     * @param isPreVote Whether the vote is a PreVote (non-binding) or not (binding)
     * @return true if the voter had not been previously recorded
     * @throws IllegalArgumentException if the remote node is not a voter or if the vote had already been
     *         rejected by this node
     */
    boolean recordGrantedVote(int remoteNodeId, boolean isPreVote) {
        VoterState voterState = voterStates.get(remoteNodeId);
        if (voterState == null) {
            throw new IllegalArgumentException("Attempt to grant vote to non-voter " + remoteNodeId);
        } else if (!isPreVote && voterState.state().equals(VoterState.State.REJECTED)) {
            throw new IllegalArgumentException("Attempt to grant vote from node " + remoteNodeId +
                " which previously rejected our request");
        }
        boolean recorded = voterState.state().equals(VoterState.State.UNRECORDED);
        voterState.setState(VoterState.State.GRANTED);

        return recorded;
    }

    /**
     * Record a rejected vote from one of the voters.
     *
     * @param remoteNodeId The id of the voter
     * @param isPreVote Whether the vote is a PreVote (non-binding) or not (binding)
     * @return true if the rejected vote had not been previously recorded
     * @throws IllegalArgumentException if the remote node is not a voter or if the vote had already been
     *         granted by this node
     */
    public boolean recordRejectedVote(int remoteNodeId, boolean isPreVote) {
        VoterState voterState = voterStates.get(remoteNodeId);
        if (voterState == null) {
            throw new IllegalArgumentException("Attempt to reject vote to non-voter " + remoteNodeId);
        } else if (isPreVote && remoteNodeId == localId) {
            throw new IllegalStateException("Attempted to reject vote from ourselves");
        } else if (!isPreVote && voterState.state().equals(VoterState.State.GRANTED)) {
            throw new IllegalArgumentException("Attempt to reject vote from node " + remoteNodeId +
                " which previously granted our request");
        }
        boolean recorded = voterState.state().equals(VoterState.State.UNRECORDED);
        voterState.setState(VoterState.State.REJECTED);

        return recorded;
    }

    /**
     * Get the set of voters which have not been counted as granted or rejected yet.
     *
     * @return The set of unrecorded voters
     */
    Set<ReplicaKey> unrecordedVoters() {
        return voters(VoterState.State.UNRECORDED).collect(Collectors.toSet());
    }

    /**
     * Get the set of voters that have granted our vote requests.
     *
     * @return The set of granting voters, which should always contain the localId
     */
    Set<Integer> grantingVoters() {
        return voters(VoterState.State.GRANTED).map(ReplicaKey::id).collect(Collectors.toSet());
    }

    /**
     * Get the set of voters that have rejected our candidacy.
     *
     * @return The set of rejecting voters
     */
    Set<Integer> rejectingVoters() {
        return voters(VoterState.State.REJECTED).map(ReplicaKey::id).collect(Collectors.toSet());
    }

    static final class VoterState {
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
