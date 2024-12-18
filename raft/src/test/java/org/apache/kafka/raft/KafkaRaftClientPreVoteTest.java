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
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.raft.RaftClientTestContext.RaftProtocol;
import org.apache.kafka.server.common.KRaftVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.raft.KafkaRaftClientTest.randomReplicaId;
import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;
import static org.apache.kafka.raft.RaftClientTestContext.RaftProtocol.KIP_996_PROTOCOL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientPreVoteTest {
    @ParameterizedTest
    @MethodSource("kraftVersionHasFetchedCombinations")
    public void testHandlePreVoteRequestAsFollowerWithElectedLeader(
        KRaftVersion kraftVersion,
        boolean hasFetchedFromLeader
    ) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        ReplicaKey electedLeader = replicaKey(localId + 2, true);
        ReplicaKey observer = replicaKey(localId + 3, true);
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(local),
            Optional.of(VoterSetTest.voterSet(Stream.of(local, otherNodeKey, electedLeader))),
            kraftVersion
        )
            .withElectedLeader(epoch, electedLeader.id())
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();

        if (hasFetchedFromLeader) {
            context.pollUntilRequest();
            RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
            context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

            context.deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                context.fetchResponse(epoch, electedLeader.id(), MemoryRecords.EMPTY, 0L, Errors.NONE)
            );
        }

        // follower should reject pre-vote requests with the same epoch if it has successfully fetched from the leader
        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, epoch, 1));
        context.pollUntilResponse();

        boolean voteGranted = !hasFetchedFromLeader;
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(electedLeader.id()), voteGranted);
        context.assertElectedLeader(epoch, electedLeader.id());

        // same with observers
        context.deliverRequest(context.preVoteRequest(epoch, observer, epoch, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(electedLeader.id()), voteGranted);
        context.assertElectedLeader(epoch, electedLeader.id());

        // follower will transition to unattached if pre-vote request has a higher epoch
        context.deliverRequest(context.preVoteRequest(epoch + 1, otherNodeKey, epoch + 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.of(-1), true);
        assertEquals(context.currentEpoch(), epoch + 1);
        assertTrue(context.client.quorum().isUnattachedNotVoted());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testHandlePreVoteRequestAsFollowerWithVotedCandidate(KRaftVersion kraftVersion) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        ReplicaKey votedCandidateKey = replicaKey(localId + 2, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(localKey),
            Optional.of(VoterSetTest.voterSet(Stream.of(localKey, otherNodeKey, votedCandidateKey))),
            kraftVersion
        )
            .withVotedCandidate(epoch, votedCandidateKey)
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();

        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, epoch, 1));
        context.pollUntilResponse();

        // follower can grant pre-votes if it has not fetched successfully from leader yet
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);
        ReplicaKey expectedVotedKey = kraftVersion == KRaftVersion.KRAFT_VERSION_1
            ? votedCandidateKey : replicaKey(votedCandidateKey.id(), false);
        context.assertVotedCandidate(epoch, expectedVotedKey);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testHandlePreVoteRequestAsCandidate(KRaftVersion kraftVersion) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        ReplicaKey observer = replicaKey(localId + 2, true);
        int leaderEpoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(localKey),
            Optional.of(VoterSetTest.voterSet(Stream.of(localKey, otherNodeKey))),
            kraftVersion
        )
            .withVotedCandidate(leaderEpoch, ReplicaKey.of(localId, localKey.directoryId().get()))
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();
        assertTrue(context.client.quorum().isCandidate());

        // candidate should grant pre-vote requests with the same epoch if log is up-to-date
        context.deliverRequest(context.preVoteRequest(leaderEpoch, otherNodeKey, leaderEpoch, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.empty(), true);
        ReplicaKey expectedVotedKey = kraftVersion == KRaftVersion.KRAFT_VERSION_1
            ? localKey : replicaKey(localId, false);
        context.assertVotedCandidate(leaderEpoch, expectedVotedKey);
        assertTrue(context.client.quorum().isCandidate());

        // if an observer sends a pre-vote request for the same epoch, it should also be granted
        context.deliverRequest(context.preVoteRequest(leaderEpoch, observer, leaderEpoch, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.empty(), true);
        context.assertVotedCandidate(leaderEpoch, expectedVotedKey);
        assertTrue(context.client.quorum().isCandidate());

        // candidate will transition to unattached if pre-vote request has a higher epoch
        context.deliverRequest(context.preVoteRequest(leaderEpoch + 1, otherNodeKey, leaderEpoch + 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch + 1, OptionalInt.of(-1), true);
        assertTrue(context.client.quorum().isUnattached());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testHandlePreVoteRequestAsUnattachedObserver(KRaftVersion kraftVersion) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        ReplicaKey observer = replicaKey(localId + 3, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(localKey),
            Optional.of(VoterSetTest.voterSet(Stream.of(replica1, replica2))),
            kraftVersion
        )
            .withUnknownLeader(epoch)
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();

        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if same replica sends another pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if different replica sends a pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica2, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if an observer sends a pre-vote request for the same epoch, it should also be granted
        context.deliverRequest(context.preVoteRequest(epoch, observer, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testHandlePreVoteRequestAsUnattachedVoted(KRaftVersion kraftVersion) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        ReplicaKey observer = replicaKey(localId + 3, true);
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(localKey),
            Optional.of(VoterSetTest.voterSet(Stream.of(replica1, replica2))),
            kraftVersion
        )
            .withVotedCandidate(epoch, replica2)
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();

        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattachedAndVoted());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if same replica sends another pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if different replica sends a pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica2, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if an observer sends a pre-vote request for the same epoch, it should also be granted
        context.deliverRequest(context.preVoteRequest(epoch, observer, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testHandlePreVoteRequestAsUnattachedWithLeader(KRaftVersion kraftVersion) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        ReplicaKey leader = replicaKey(localId + 3, true);
        ReplicaKey observer = replicaKey(localId + 4, true);
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(localKey),
            Optional.of(VoterSetTest.voterSet(Stream.of(replica1, replica2))),
            kraftVersion
        )
            .withElectedLeader(epoch, leader.id())
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();

        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattachedNotVoted());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()), true);

        // if same replica sends another pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()), true);

        // if different replica sends a pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica2, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()), true);

        // if an observer sends a pre-vote request for the same epoch, it should also be granted
        context.deliverRequest(context.preVoteRequest(epoch, observer, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()), true);
    }

    @ParameterizedTest
    @MethodSource("kraftVersionHasFetchedCombinations")
    public void testHandlePreVoteRequestAsFollowerObserverThatHasNotFetched(
        KRaftVersion kraftVersion,
        boolean hasFetchedFromLeader
    ) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey leader = replicaKey(localId + 1, true);
        ReplicaKey follower = replicaKey(localId + 2, true);
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(localKey),
            Optional.of(VoterSetTest.voterSet(Stream.of(localKey, leader, follower))),
            kraftVersion
        )
            .withElectedLeader(epoch, leader.id())
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();
        context.assertElectedLeader(epoch, leader.id());

        if (hasFetchedFromLeader) {
            context.pollUntilRequest();
            RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
            context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

            context.deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                context.fetchResponse(epoch, leader.id(), MemoryRecords.EMPTY, 0L, Errors.NONE)
            );
        }

        context.deliverRequest(context.preVoteRequest(epoch, follower, epoch, 1));
        context.pollUntilResponse();

        boolean voteGranted = !hasFetchedFromLeader;
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()), voteGranted);
        assertTrue(context.client.quorum().isFollower());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testHandleInvalidPreVoteRequestWithOlderEpoch() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();

        context.deliverRequest(context.preVoteRequest(epoch - 1, otherNodeKey, epoch - 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.empty(), false);
        context.assertUnknownLeaderAndNoVotedCandidate(epoch);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderRejectPreVoteRequestOnSameEpoch() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, localKey.directoryId().get())
            .withUnknownLeader(2)
            .withRaftProtocol(KIP_996_PROTOCOL)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(localKey, otherNodeKey))))
            .build();

        context.unattachedToLeader();
        int leaderEpoch = context.currentEpoch();

        context.deliverRequest(context.preVoteRequest(leaderEpoch, otherNodeKey, leaderEpoch, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.of(localId), false);
        context.assertElectedLeader(leaderEpoch, localId);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testPreVoteRequestClusterIdValidation() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, localKey.directoryId().get())
            .withRaftProtocol(KIP_996_PROTOCOL)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(localKey, otherNodeKey))))
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // valid cluster id is accepted
        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, epoch, 0));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), false);

        // null cluster id is accepted
        context.deliverRequest(context.voteRequest(null, epoch, otherNodeKey, epoch, 0, true));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), false);

        // empty cluster id is rejected
        context.deliverRequest(context.voteRequest("", epoch, otherNodeKey, epoch, 0, true));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INCONSISTENT_CLUSTER_ID);

        // invalid cluster id is rejected
        context.deliverRequest(context.voteRequest("invalid-uuid", epoch, otherNodeKey, epoch, 0, true));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INCONSISTENT_CLUSTER_ID);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInvalidVoterReplicaPreVoteRequest() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, localKey.directoryId().get())
            .withRaftProtocol(KIP_996_PROTOCOL)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(localKey, otherNodeKey))))
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // invalid voter id is rejected
        context.deliverRequest(
            context.voteRequest(
                context.clusterId.toString(),
                epoch,
                otherNodeKey,
                ReplicaKey.of(10, Uuid.randomUuid()),
                epoch,
                100,
                true
            )
        );
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INVALID_VOTER_KEY, epoch, OptionalInt.of(localId), false);

        // invalid voter directory id is rejected
        context.deliverRequest(
            context.voteRequest(
                context.clusterId.toString(),
                epoch,
                otherNodeKey,
                ReplicaKey.of(0, Uuid.randomUuid()),
                epoch,
                100,
                true
            )
        );
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INVALID_VOTER_KEY, epoch, OptionalInt.of(localId), false);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderAcceptPreVoteFromObserver() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, localKey.directoryId().get())
            .withUnknownLeader(4)
            .withRaftProtocol(KIP_996_PROTOCOL)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(localKey, otherNodeKey))))
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey observerKey = replicaKey(localId + 2, true);
        context.deliverRequest(context.preVoteRequest(epoch - 1, observerKey, 0, 0));
        context.client.poll();
        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.of(localId), false);

        context.deliverRequest(context.preVoteRequest(epoch, observerKey, 0, 0));
        context.client.poll();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), false);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testHandlePreVoteRequestAsResigned() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();
        context.unattachedToLeader();
        context.client.quorum().transitionToResigned(Collections.emptyList());
        assertTrue(context.client.quorum().isResigned());

        // resigned should grant pre-vote requests with the same epoch if log is up-to-date
        int epoch = context.currentEpoch();
        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, epoch, 1));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), true);

        // resigned will transition to unattached if pre-vote request has a higher epoch
        context.deliverRequest(context.preVoteRequest(epoch + 1, otherNodeKey, epoch + 1, 1));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.of(-1), true);
        assertTrue(context.client.quorum().isUnattached());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInvalidPreVoteRequest() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeKey.id())
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();
        assertEquals(epoch, context.currentEpoch());
        context.assertElectedLeader(epoch, otherNodeKey.id());

        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, 0, -5L));
        context.pollUntilResponse();
        context.assertSentVoteResponse(
            Errors.INVALID_REQUEST,
            epoch,
            OptionalInt.of(otherNodeKey.id()),
            false
        );
        assertEquals(epoch, context.currentEpoch());
        context.assertElectedLeader(epoch, otherNodeKey.id());

        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, -1, 0L));
        context.pollUntilResponse();
        context.assertSentVoteResponse(
            Errors.INVALID_REQUEST,
            epoch,
            OptionalInt.of(otherNodeKey.id()),
            false
        );
        assertEquals(epoch, context.currentEpoch());
        context.assertElectedLeader(epoch, otherNodeKey.id());

        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, epoch + 1, 0L));
        context.pollUntilResponse();
        context.assertSentVoteResponse(
            Errors.INVALID_REQUEST,
            epoch,
            OptionalInt.of(otherNodeKey.id()),
            false
        );
        assertEquals(epoch, context.currentEpoch());
        context.assertElectedLeader(epoch, otherNodeKey.id());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerGrantsPreVoteIfHasNotFetchedYet(KRaftVersion kraftVersion) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(local),
            Optional.of(VoterSetTest.voterSet(Stream.of(replica1, replica2))),
            kraftVersion
        )
            .withElectedLeader(epoch, replica1.id())
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();

        context.assertElectedLeader(epoch, replica1.id());

        assertTrue(context.client.quorum().isFollower());

        // We will grant PreVotes before fetching successfully from the leader, it will NOT contain the leaderId
        context.deliverRequest(context.preVoteRequest(epoch, replica2, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isFollower());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(replica1.id()), true);

        // After fetching successfully from the leader once, we will no longer grant PreVotes
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, replica1.id(), MemoryRecords.EMPTY, 0L, Errors.NONE)
        );
        assertTrue(context.client.quorum().isFollower());

        context.deliverRequest(context.preVoteRequest(epoch, replica2, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isFollower());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testRejectPreVoteIfRemoteLogIsNotUpToDate(KRaftVersion kraftVersion) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(local),
            Optional.of(VoterSetTest.voterSet(Stream.of(local, replica1, replica2))),
            kraftVersion
        )
            .withUnknownLeader(epoch)
            .withRaftProtocol(KIP_996_PROTOCOL)
            .appendToLog(epoch, Arrays.asList("a", "b", "c"))
            .build();
        assertTrue(context.client.quorum().isUnattached());
        assertEquals(3, context.log.endOffset().offset());

        // older epoch
        context.deliverRequest(context.preVoteRequest(epoch - 1, replica1, epoch - 1, 0));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.empty(), false);

        // older offset
        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch - 1, context.log.endOffset().offset() - 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), false);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testPreVoteResponseIgnoredAfterBecomingFollower(KRaftVersion kraftVersion) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey voter2 = replicaKey(localId + 1, true);
        ReplicaKey voter3 = replicaKey(localId + 2, true);
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(local),
            Optional.of(VoterSetTest.voterSet(Stream.of(local, voter2, voter3))),
            kraftVersion
        )
            .withUnknownLeader(epoch)
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();

        context.assertUnknownLeaderAndNoVotedCandidate(epoch);

        // Sleep a little to ensure that we become a prospective
        context.time.sleep(context.electionTimeoutMs() * 2L);

        // Wait until the vote requests are inflight
        context.pollUntilRequest();
        assertTrue(context.client.quorum().isProspective());
        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(epoch, 0, 0);
        assertEquals(2, voteRequests.size());

        // While the vote requests are still inflight, we receive a BeginEpoch for the same epoch
        context.deliverRequest(context.beginEpochRequest(epoch, voter3.id()));
        context.client.poll();
        context.assertElectedLeader(epoch, voter3.id());

        // If PreVote responses are received now they should be ignored
        VoteResponseData voteResponse1 = context.voteResponse(true, OptionalInt.empty(), epoch);
        context.deliverResponse(
            voteRequests.get(0).correlationId(),
            voteRequests.get(0).destination(),
            voteResponse1
        );

        VoteResponseData voteResponse2 = context.voteResponse(true, OptionalInt.of(voter3.id()), epoch);
        context.deliverResponse(
            voteRequests.get(1).correlationId(),
            voteRequests.get(1).destination(),
            voteResponse2
        );

        context.client.poll();
        context.assertElectedLeader(epoch, voter3.id());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testPreVoteNotSupportedByRemote(KRaftVersion kraftVersion) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey voter2Key = replicaKey(localId + 1, true);
        ReplicaKey voter3Key = replicaKey(localId + 2, true);
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(local),
            Optional.of(VoterSetTest.voterSet(Stream.of(local, voter2Key, voter3Key))),
            kraftVersion
        )
            .withUnknownLeader(epoch)
            .withRaftProtocol(KIP_996_PROTOCOL)
            .build();

        context.assertUnknownLeaderAndNoVotedCandidate(epoch);

        // Sleep a little to ensure that we transition to Prospective
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();
        assertEquals(epoch, context.currentEpoch());
        assertTrue(context.client.quorum().isProspective());

        // Simulate one remote node not supporting PreVote with UNSUPPORTED_VERSION response.
        // Note: with the mocked network client we simulate this is a bit differently, in reality this response would
        // be generated from the network client and not sent from the remote node.
        List<RaftRequest.Outbound> voteRequests = context.collectPreVoteRequests(epoch, 0, 0);
        assertEquals(2, voteRequests.size());
        context.deliverResponse(
            voteRequests.get(0).correlationId(),
            voteRequests.get(0).destination(),
            RaftUtil.errorResponse(ApiKeys.VOTE, Errors.UNSUPPORTED_VERSION)
        );

        // Local should transition to Candidate since it realizes remote node does not support PreVote.
        context.client.poll();
        assertEquals(epoch + 1, context.currentEpoch());
        context.client.quorum().isCandidate();

        // Any further PreVote requests should be ignored
        context.deliverResponse(
            voteRequests.get(1).correlationId(),
            voteRequests.get(1).destination(),
            context.voteResponse(true, OptionalInt.empty(), epoch)
        );
        context.client.poll();
        assertEquals(epoch + 1, context.currentEpoch());
        context.client.quorum().isCandidate();

        // Sleep to transition back to Prospective
        context.time.sleep(context.client.quorum().candidateStateOrThrow().remainingElectionTimeMs(context.time.milliseconds()));
        context.client.poll();
        context.time.sleep(context.client.quorum().candidateStateOrThrow().remainingBackoffMs(context.time.milliseconds()));
        context.collectVoteRequests(epoch + 1, 0, 0);
        context.client.poll();
        assertEquals(epoch + 1, context.currentEpoch());
        assertTrue(context.client.quorum().isProspective());

        // Simulate receiving enough valid PreVote responses for election to succeed
        context.pollUntilRequest();
        voteRequests = context.collectPreVoteRequests(epoch + 1, 0, 0);
        assertEquals(2, voteRequests.size());
        context.deliverResponse(
            voteRequests.get(0).correlationId(),
            voteRequests.get(0).destination(),
            context.voteResponse(true, OptionalInt.empty(), epoch + 1)
        );
        context.client.poll();
        assertEquals(epoch + 2, context.currentEpoch());
        context.client.quorum().isCandidate();

        // Any further PreVote requests should be ignored
        context.deliverResponse(
            voteRequests.get(1).correlationId(),
            voteRequests.get(1).destination(),
            RaftUtil.errorResponse(ApiKeys.VOTE, Errors.UNSUPPORTED_VERSION)
        );
        context.client.poll();
        assertEquals(epoch + 2, context.currentEpoch());
        context.client.quorum().isCandidate();
    }

    @ParameterizedTest
    @MethodSource("kraftVersionRaftProtocolCombinations")
    public void testProspectiveReceivesBeginQuorumRequest(
        KRaftVersion kraftVersion,
        RaftProtocol raftProtocol
    ) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey leader = replicaKey(localId + 1, true);
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(local),
            Optional.of(VoterSetTest.voterSet(Stream.of(local, leader))),
            kraftVersion
        )
            .withUnknownLeader(epoch)
            .withRaftProtocol(raftProtocol)
            .build();

        context.assertUnknownLeaderAndNoVotedCandidate(epoch);

        // Sleep a little to ensure that we transition to Prospective
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();

        assertTrue(context.client.quorum().isProspective());

        context.deliverRequest(context.beginEpochRequest(epoch, leader.id()));
        context.client.poll();

        assertTrue(context.client.quorum().isFollower());
        context.assertElectedLeader(epoch, leader.id());
    }

    @ParameterizedTest
    @MethodSource("kraftVersionRaftProtocolCombinations")
    public void testProspectiveTransitionsToUnattachedOnElectionFailure(
        KRaftVersion kraftVersion,
        RaftProtocol raftProtocol
    ) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey otherNode = replicaKey(localId + 1, true);
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(local),
            Optional.of(VoterSetTest.voterSet(Stream.of(local, otherNode))),
            kraftVersion
        )
            .withUnknownLeader(epoch)
            .withRaftProtocol(raftProtocol)
            .build();
        context.assertUnknownLeaderAndNoVotedCandidate(epoch);

        // Sleep a little to ensure that we transition to Prospective
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();
        assertTrue(context.client.quorum().isProspective());
        context.assertSentVoteRequest(epoch, 0, 0L, 1);

        // If election timeout expires, we should transition to Unattached to attempt re-discovering leader
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.client.poll();
        assertTrue(context.client.quorum().isUnattached());

        // After election times out again, we will transition back to Prospective and continue sending PreVote requests
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();
        RaftRequest.Outbound voteRequest = context.assertSentVoteRequest(epoch, 0, 0L, 1);

        // If we receive enough rejected votes, we also transition to Unattached immediately
        context.deliverResponse(
            voteRequest.correlationId(),
            voteRequest.destination(),
            context.voteResponse(false, OptionalInt.empty(), epoch));
        // handle vote response and mark we should transition out of prospective
        context.client.poll();
        // transition
        context.client.poll();
        assertTrue(context.client.quorum().isUnattached());

        // After election times out again, we will transition back to Prospective and continue sending PreVote requests
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();
        voteRequest = context.assertSentVoteRequest(epoch, 0, 0L, 1);

        // If we receive vote response with different leaderId but empty leader endpoint, we will transition to
        // unattached with leader immediately
        context.deliverResponse(
            voteRequest.correlationId(),
            voteRequest.destination(),
            context.voteResponse(false, OptionalInt.of(localId + 2), epoch + 1));
        context.client.poll();
        assertTrue(context.client.quorum().isUnattached());
        assertEquals(epoch + 1, context.currentEpoch());
        context.assertElectedLeader(epoch + 1, localId + 2);
    }

    @ParameterizedTest
    @MethodSource("kraftVersionRaftProtocolCombinations")
    public void testProspectiveWithLeaderTransitionsToFollower(
        KRaftVersion kraftVersion,
        RaftProtocol raftProtocol
    ) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(local),
            Optional.of(VoterSetTest.voterSet(Stream.of(local, replica1, replica2))),
            kraftVersion
        )
            .withElectedLeader(epoch, replica1.id())
            .withRaftProtocol(raftProtocol)
            .build();
        context.assertElectedLeader(epoch, replica1.id());
        assertTrue(context.client.quorum().isFollower());

        // Sleep a little to ensure that we transition to Prospective
        context.time.sleep(context.fetchTimeoutMs);
        context.pollUntilRequest();
        assertTrue(context.client.quorum().isProspective());
        context.assertSentVoteRequest(epoch, 0, 0L, 2);

        // If election timeout expires, we should transition back to Follower if we haven't found new leader yet
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();
        context.assertSentFetchRequest();
        assertTrue(context.client.quorum().isFollower());
        context.assertElectedLeader(epoch, replica1.id());

        // After election times out again, we will transition back to Prospective and continue sending PreVote requests
        context.time.sleep(context.fetchTimeoutMs);
        context.pollUntilRequest();
        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(epoch, 0, 0);
        assertEquals(2, voteRequests.size());
        assertTrue(context.client.quorum().isProspective());
        context.assertElectedLeader(epoch, replica1.id());

        // If we receive enough rejected votes without leaderId, we also transition to Follower immediately
        context.deliverResponse(
            voteRequests.get(0).correlationId(),
            voteRequests.get(0).destination(),
            context.voteResponse(false, OptionalInt.empty(), epoch));
        context.deliverResponse(
            voteRequests.get(1).correlationId(),
            voteRequests.get(1).destination(),
            context.voteResponse(false, OptionalInt.empty(), epoch));
        // handle vote response and mark we should transition out of prospective
        context.client.poll();
        // transition
        context.pollUntilRequest();
        assertTrue(context.client.quorum().isFollower());
        context.assertSentFetchRequest();

        // After election times out again, we will transition back to Prospective and continue sending PreVote requests
        context.time.sleep(context.fetchTimeoutMs);
        context.pollUntilRequest();
        voteRequests = context.collectVoteRequests(epoch, 0, 0);
        assertEquals(2, voteRequests.size());
        assertTrue(context.client.quorum().isProspective());
        context.assertElectedLeader(epoch, replica1.id());

        // If we receive vote response with different leaderId, we will transition to follower immediately
        context.deliverResponse(
            voteRequests.get(0).correlationId(),
            voteRequests.get(0).destination(),
            context.voteResponse(false, OptionalInt.of(replica2.id()), epoch + 1));
        context.client.poll();
        assertTrue(context.client.quorum().isFollower());
        context.assertElectedLeader(epoch + 1, replica2.id());
    }

    @ParameterizedTest
    @MethodSource("kraftVersionRaftProtocolCombinations")
    public void testProspectiveWithoutLeaderTransitionsToFollower(
        KRaftVersion kraftVersion,
        RaftProtocol raftProtocol
    ) throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey leader = replicaKey(local.id() + 1, true);
        ReplicaKey follower = replicaKey(local.id() + 2, true);
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            Optional.of(local),
            Optional.of(VoterSetTest.voterSet(Stream.of(local, leader, follower))),
            kraftVersion
        )
            .withUnknownLeader(epoch)
            .withRaftProtocol(raftProtocol)
            .build();
        context.assertUnknownLeaderAndNoVotedCandidate(epoch);

        // Sleep a little to ensure that we transition to Prospective
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();

        assertTrue(context.client.quorum().isProspective());

        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(epoch, 0, 0);
        assertEquals(2, voteRequests.size());

        // Simulate PreVote response with granted=true and a leaderId
        VoteResponseData voteResponse1 = context.voteResponse(true, OptionalInt.of(leader.id()), epoch);
        context.deliverResponse(
            voteRequests.get(0).correlationId(),
            voteRequests.get(0).destination(),
            voteResponse1
        );

        // Prospective should transition to Follower
        context.client.poll();
        assertTrue(context.client.quorum().isFollower());
        assertEquals(leader.id(), context.client.quorum().leaderId().orElse(-1));
    }

    static Stream<Arguments> kraftVersionRaftProtocolCombinations() {
        return Stream.of(KRaftVersion.values())
            .flatMap(enum1 -> Stream.of(RaftProtocol.values())
                .map(enum2 -> Arguments.of(enum1, enum2)));
    }

    static Stream<Arguments> kraftVersionHasFetchedCombinations() {
        return Stream.of(KRaftVersion.values())
            .flatMap(enum1 -> Stream.of(true, false)
                .map(enum2 -> Arguments.of(enum1, enum2)));
    }
}
