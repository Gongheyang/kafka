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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.raft.internals.EpochElection;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EpochElectionTest {
    static final int VOTER_1_ID = randomReplicaId();
    static final Set<ReplicaKey> VOTERS = Set.of(
        ReplicaKey.of(VOTER_1_ID, Uuid.randomUuid()),
        ReplicaKey.of(VOTER_1_ID + 1, Uuid.randomUuid()),
        ReplicaKey.of(VOTER_1_ID + 2, Uuid.randomUuid())
    );
    @Test
    public void testStateOnInitialization() {
        EpochElection epochElection = new EpochElection(VOTERS);

        assertEquals(VOTERS, epochElection.unrecordedVoters());
        assertTrue(epochElection.grantingVoters().isEmpty());
        assertTrue(epochElection.rejectingVoters().isEmpty());
        assertFalse(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());
        assertFalse(epochElection.isGrantedVoter(VOTER_1_ID));
        assertFalse(epochElection.isRejectedVoter(VOTER_1_ID));
    }

    @Test
    public void testRecordGrantedVote() {
        EpochElection epochElection = new EpochElection(VOTERS);

        assertTrue(epochElection.recordVote(VOTER_1_ID, true));
        assertEquals(1, epochElection.grantingVoters().size());
        assertTrue(epochElection.grantingVoters().contains(VOTER_1_ID));
        assertEquals(0, epochElection.rejectingVoters().size());
        assertEquals(2, epochElection.unrecordedVoters().size());
        assertTrue(epochElection.isGrantedVoter(VOTER_1_ID));
        assertFalse(epochElection.isRejectedVoter(VOTER_1_ID));
        assertFalse(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());

        // recording same id as granted
        assertFalse(epochElection.recordVote(VOTER_1_ID, true));
        assertTrue(epochElection.isGrantedVoter(VOTER_1_ID));
        assertFalse(epochElection.isVoteGranted());

        // recording majority as granted
        assertTrue(epochElection.recordVote(VOTER_1_ID + 1, true));
        assertEquals(2, epochElection.grantingVoters().size());
        assertEquals(0, epochElection.rejectingVoters().size());
        assertEquals(1, epochElection.unrecordedVoters().size());
        assertTrue(epochElection.isGrantedVoter(VOTER_1_ID + 1));
        assertFalse(epochElection.isRejectedVoter(VOTER_1_ID + 1));
        assertTrue(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());
    }

    @Test
    public void testRecordRejectedVote() {
        EpochElection epochElection = new EpochElection(VOTERS);

        assertTrue(epochElection.recordVote(VOTER_1_ID, false));
        assertEquals(0, epochElection.grantingVoters().size());
        assertEquals(1, epochElection.rejectingVoters().size());
        assertTrue(epochElection.rejectingVoters().contains(VOTER_1_ID));
        assertEquals(2, epochElection.unrecordedVoters().size());
        assertFalse(epochElection.isGrantedVoter(VOTER_1_ID));
        assertTrue(epochElection.isRejectedVoter(VOTER_1_ID));
        assertFalse(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());

        // recording same id as rejected
        assertFalse(epochElection.recordVote(VOTER_1_ID, false));
        assertFalse(epochElection.isGrantedVoter(VOTER_1_ID));
        assertFalse(epochElection.isVoteRejected());

        // recording majority as rejected
        assertTrue(epochElection.recordVote(VOTER_1_ID + 1, false));
        assertEquals(0, epochElection.grantingVoters().size());
        assertEquals(2, epochElection.rejectingVoters().size());
        assertEquals(1, epochElection.unrecordedVoters().size());
        assertFalse(epochElection.isGrantedVoter(VOTER_1_ID + 1));
        assertTrue(epochElection.isRejectedVoter(VOTER_1_ID + 1));
        assertFalse(epochElection.isVoteGranted());
        assertTrue(epochElection.isVoteRejected());
    }

    @Test
    public void testOverWritingVote() {
        EpochElection epochElection = new EpochElection(VOTERS);

        assertTrue(epochElection.recordVote(VOTER_1_ID, true));
        assertFalse(epochElection.recordVote(VOTER_1_ID, false));
        assertEquals(0, epochElection.grantingVoters().size());
        assertEquals(1, epochElection.rejectingVoters().size());
        assertTrue(epochElection.rejectingVoters().contains(VOTER_1_ID));
        assertFalse(epochElection.isGrantedVoter(VOTER_1_ID));
        assertTrue(epochElection.isRejectedVoter(VOTER_1_ID));
        assertFalse(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());

        assertTrue(epochElection.recordVote(VOTER_1_ID + 2, false));
        assertFalse(epochElection.recordVote(VOTER_1_ID + 2, true));
        assertEquals(1, epochElection.grantingVoters().size());
        assertEquals(1, epochElection.rejectingVoters().size());
        assertTrue(epochElection.grantingVoters().contains(VOTER_1_ID + 2));
        assertTrue(epochElection.isGrantedVoter(VOTER_1_ID + 2));
        assertFalse(epochElection.isRejectedVoter(VOTER_1_ID + 2));
        assertFalse(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());
    }

    static int randomReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }
}
