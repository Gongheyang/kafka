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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProspectiveStateWithVoteTest {

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private final int localId = 0;
    private final ReplicaKey localReplicaKey = ReplicaKey.of(localId, Uuid.randomUuid());
    private final int epoch = 5;
    private final int votedId = 1;
    private final Uuid votedDirectoryId = Uuid.randomUuid();
    private final ReplicaKey votedKeyWithDirectoryId = ReplicaKey.of(votedId, votedDirectoryId);
    private final ReplicaKey votedKeyWithoutDirectoryId = ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID);
    private final int electionTimeoutMs = 10000;

    private ProspectiveState newProspectiveVotedState(
        VoterSet voters,
        Optional<ReplicaKey> votedKey
    ) {
        return new ProspectiveState(
            time,
            localId,
            epoch,
            votedKey,
            voters,
            Optional.empty(),
            0,
            electionTimeoutMs,
            logContext
        );
    }

    @Test
    public void testElectionTimeout() {
        ProspectiveState state = newProspectiveVotedState(voterSetWithLocal(IntStream.empty(), true), Optional.of(votedKeyWithDirectoryId));

        assertEquals(epoch, state.epoch());
        assertEquals(votedKeyWithDirectoryId, state.votedKey().get());
        assertEquals(
            ElectionState.withVotedCandidate(epoch, votedKeyWithDirectoryId, Collections.singleton(localId)),
            state.election()
        );
        assertEquals(electionTimeoutMs, state.remainingElectionTimeMs(time.milliseconds()));
        assertFalse(state.hasElectionTimeoutExpired(time.milliseconds()));

        time.sleep(5000);
        assertEquals(electionTimeoutMs - 5000, state.remainingElectionTimeMs(time.milliseconds()));
        assertFalse(state.hasElectionTimeoutExpired(time.milliseconds()));

        time.sleep(5000);
        assertEquals(0, state.remainingElectionTimeMs(time.milliseconds()));
        assertTrue(state.hasElectionTimeoutExpired(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCanGrantVoteWithoutDirectoryId(boolean isLogUpToDate) {
        ProspectiveState state = newProspectiveVotedState(voterSetWithLocal(IntStream.empty(), true), Optional.of(votedKeyWithoutDirectoryId));

        assertTrue(state.canGrantPreVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate));
        assertTrue(state.canGrantVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate));

        assertTrue(state.canGrantPreVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate));
        assertTrue(state.canGrantVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate));

        // Can grant PreVote to other replicas even if we have granted a standard vote to another replica
        assertEquals(
            isLogUpToDate,
            state.canGrantPreVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCanGrantVoteWithDirectoryId(boolean isLogUpToDate) {
        ProspectiveState state = newProspectiveVotedState(voterSetWithLocal(IntStream.empty(), true), Optional.of(votedKeyWithDirectoryId));

        // Same voterKey
        // We will not grant PreVote for a replica we have already granted a standard vote to if their log is behind
        assertEquals(
            isLogUpToDate,
            state.canGrantPreVote(votedKeyWithDirectoryId, isLogUpToDate)
        );
        assertTrue(state.canGrantVote(votedKeyWithDirectoryId, isLogUpToDate));

        // Different directoryId
        // We can grant PreVote for a replica we have already granted a standard vote to if their log is up-to-date,
        // even if the directoryId is different
        assertEquals(
            isLogUpToDate,
            state.canGrantPreVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate));

        // Missing directoryId
        assertEquals(
            isLogUpToDate,
            state.canGrantPreVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate));

        // Different voterId
        assertEquals(
            isLogUpToDate,
            state.canGrantPreVote(ReplicaKey.of(votedId + 1, votedDirectoryId), isLogUpToDate)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantPreVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId + 1, votedDirectoryId), true));
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), true));
    }

    @Test
    void testLeaderEndpoints() {
        ProspectiveState state = newProspectiveVotedState(voterSetWithLocal(IntStream.empty(), true), Optional.of(votedKeyWithDirectoryId));

        assertEquals(Endpoints.empty(), state.leaderEndpoints());
    }

    private ReplicaKey replicaKey(int id, boolean withDirectoryId) {
        Uuid directoryId = withDirectoryId ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID;
        return ReplicaKey.of(id, directoryId);
    }

    private VoterSet voterSetWithLocal(IntStream remoteVoterIds, boolean withDirectoryId) {
        Stream<ReplicaKey> remoteVoterKeys = remoteVoterIds
            .boxed()
            .map(id -> replicaKey(id, withDirectoryId));

        return voterSetWithLocal(remoteVoterKeys, withDirectoryId);
    }

    private VoterSet voterSetWithLocal(Stream<ReplicaKey> remoteVoterKeys, boolean withDirectoryId) {
        ReplicaKey actualLocalVoter = withDirectoryId ?
            localReplicaKey :
            ReplicaKey.of(localReplicaKey.id(), ReplicaKey.NO_DIRECTORY_ID);

        return VoterSetTest.voterSet(
            Stream.concat(Stream.of(actualLocalVoter), remoteVoterKeys)
        );
    }
}

