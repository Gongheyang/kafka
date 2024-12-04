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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.raft.KafkaRaftClientTest.randomReplicaId;
import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientPreVoteTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHandlePreVoteRequestAsFollowerWithElectedLeader(boolean hasFetchedFromLeader) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        ReplicaKey observer = replicaKey(localId + 2, true);
        int electedLeaderId = localId + 2;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id(), electedLeaderId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, electedLeaderId)
            .withKip853Rpc(true)
            .build();

        if (hasFetchedFromLeader) {
            context.pollUntilRequest();
            RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
            context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

            context.deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                context.fetchResponse(epoch, electedLeaderId, MemoryRecords.EMPTY, 0L, Errors.NONE)
            );
        }

        // follower should reject pre-vote requests with the same epoch if it has successfully fetched from the leader
        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, epoch, 1));
        context.pollUntilResponse();

        boolean voteGranted = !hasFetchedFromLeader;
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(electedLeaderId), voteGranted);
        context.assertElectedLeader(epoch, electedLeaderId);

        // same with observers
        context.deliverRequest(context.preVoteRequest(epoch, observer, epoch, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(electedLeaderId), voteGranted);
        context.assertElectedLeader(epoch, electedLeaderId);

        // follower will transition to unattached if pre-vote request has a higher epoch
        context.deliverRequest(context.preVoteRequest(epoch + 1, otherNodeKey, epoch + 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.of(-1), true);
        assertEquals(context.currentEpoch(), epoch + 1);
        assertTrue(context.client.quorum().isUnattachedNotVoted());
    }

    @Test
    public void testHandlePreVoteRequestAsCandidate() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        ReplicaKey observer = replicaKey(localId + 2, true);
        int leaderEpoch = 2;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(leaderEpoch, ReplicaKey.of(localId, ReplicaKey.NO_DIRECTORY_ID))
            .withKip853Rpc(true)
            .build();
        assertTrue(context.client.quorum().isCandidate());

        // candidate should grant pre-vote requests with the same epoch if log is up-to-date
        context.deliverRequest(context.preVoteRequest(leaderEpoch, otherNodeKey, leaderEpoch, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.empty(), true);
        context.assertVotedCandidate(leaderEpoch, localId);
        assertTrue(context.client.quorum().isCandidate());

        // if an observer sends a pre-vote request for the same epoch, it should also be granted
        context.deliverRequest(context.preVoteRequest(leaderEpoch, observer, leaderEpoch, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.empty(), true);
        context.assertVotedCandidate(leaderEpoch, localId);
        assertTrue(context.client.quorum().isCandidate());

        // candidate will transition to unattached if pre-vote request has a higher epoch
        context.deliverRequest(context.preVoteRequest(leaderEpoch + 1, otherNodeKey, leaderEpoch + 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch + 1, OptionalInt.of(-1), true);
        assertTrue(context.client.quorum().isUnattached());
    }

    @Test
    public void testHandlePreVoteRequestAsUnattachedObserver() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        ReplicaKey observer = replicaKey(localId + 3, true);
        Set<Integer> voters = Set.of(replica1.id(), replica2.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(true)
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

    @Test
    public void testHandlePreVoteRequestAsUnattachedVoted() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        ReplicaKey observer = replicaKey(localId + 3, true);
        Set<Integer> voters = Set.of(replica1.id(), replica2.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(epoch, replica2)
            .withKip853Rpc(true)
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

    @Test
    public void testHandlePreVoteRequestAsUnattachedWithLeader() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        ReplicaKey leader = replicaKey(localId + 3, true);
        ReplicaKey observer = replicaKey(localId + 4, true);
        Set<Integer> voters = Set.of(replica1.id(), replica2.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leader.id())
            .withKip853Rpc(true)
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
    @ValueSource(booleans = {true, false})
    public void testHandlePreVoteRequestAsFollowerObserver(boolean hasFetchedFromLeader) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        int leaderId = localId + 1;
        ReplicaKey leader = replicaKey(leaderId, true);
        ReplicaKey follower = replicaKey(localId + 2, true);
        Set<Integer> voters = Set.of(leader.id(), follower.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leader.id())
            .withKip853Rpc(true)
            .build();
        context.assertElectedLeader(epoch, leader.id());

        if (hasFetchedFromLeader) {
            context.pollUntilRequest();
            RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
            context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

            context.deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.NONE)
            );
        }

        context.deliverRequest(context.preVoteRequest(epoch, follower, epoch, 1));
        context.pollUntilResponse();

        boolean voteGranted = !hasFetchedFromLeader;
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(leaderId), voteGranted);
        assertTrue(context.client.quorum().isFollower());
    }

    @Test
    public void testHandleInvalidPreVoteRequestWithOlderEpoch() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(true)
            .build();

        context.deliverRequest(context.preVoteRequest(epoch - 1, otherNodeKey, epoch - 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.empty(), false);
        context.assertUnknownLeader(epoch);
    }

    @Test
    public void testLeaderRejectPreVoteRequestOnSameEpoch() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, localKey.directoryId().get())
            .withUnknownLeader(2)
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(localKey, otherNodeKey))))
            .build();

        context.becomeLeader();
        int leaderEpoch = context.currentEpoch();

        context.deliverRequest(context.preVoteRequest(leaderEpoch, otherNodeKey, leaderEpoch, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.of(localId), false);
        context.assertElectedLeader(leaderEpoch, localId);
    }

    @Test
    public void testPreVoteRequestClusterIdValidation() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, localKey.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(localKey, otherNodeKey))))
            .build();

        context.becomeLeader();
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

    @Test
    public void testInvalidVoterReplicaPreVoteRequest() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, localKey.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(localKey, otherNodeKey))))
            .build();

        context.becomeLeader();
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

    @Test
    public void testLeaderAcceptPreVoteFromObserver() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey localKey = replicaKey(localId, true);
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, localKey.directoryId().get())
            .withUnknownLeader(4)
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(localKey, otherNodeKey))))
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey observerKey = replicaKey(localId + 2, true);
        context.deliverRequest(context.preVoteRequest(epoch - 1, observerKey, 0, 0));
        context.client.poll();
        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.of(localId), false);

        context.deliverRequest(context.preVoteRequest(epoch, observerKey, 0, 0));
        context.client.poll();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), false);
    }

    @Test
    public void testHandlePreVoteRequestAsResigned() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(true)
            .build();
        context.becomeLeader();
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

    @Test
    public void testInvalidVoteRequest() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeKey.id())
            .withKip853Rpc(true)
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

    @Test
    public void testFollowerGrantsPreVoteIfHasNotFetchedYet() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        Set<Integer> voters = Set.of(replica1.id(), replica2.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, replica1.id())
            .withKip853Rpc(true)
            .build();
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

    @Test
    public void testRejectPreVoteIfRemoteLogIsNotUpToDate() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        Set<Integer> voters = Set.of(localId, replica1.id(), replica2.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(true)
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

    @Test
    public void testPreVoteResponseIgnoredAfterBecomingFollower() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey voter2 = replicaKey(localId + 1, true);
        ReplicaKey voter3 = replicaKey(localId + 2, true);
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, local.directoryId().get())
            .withUnknownLeader(epoch)
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(local, voter2, voter3))))
            .build();

        context.assertUnknownLeader(epoch);

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

    @Test
    public void testStaticQuorumDoesNotSendPreVoteRequest() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey voter2Bootstrap = replicaKey(localId + 1, true);
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, local.directoryId().get())
            .withUnknownLeader(epoch)
            .withStaticVoters(VoterSetTest.voterSet(Stream.of(local, voter2Bootstrap)))
            .build();

        context.assertUnknownLeader(epoch);

        // Sleep a little to ensure that we transition to a Voting state
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();

        // We should transition to Candidate state
        assertTrue(context.client.quorum().isCandidate());

        // Candidate state should not send PreVote requests
        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(epoch + 1, 0, 0);
        assertEquals(1, voteRequests.size());
        context.assertVotedCandidate(epoch + 1, localId);
    }

    @Test
    public void testPreVoteNotSupportedByRemote() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        Uuid localDirectoryId = local.directoryId().orElse(Uuid.randomUuid());
        int voter2 = localId + 1;
        ReplicaKey voter2Key = replicaKey(voter2, true);
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, localDirectoryId)
            .withUnknownLeader(epoch)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(local, voter2Key))))
            .build();

        context.assertUnknownLeader(epoch);

        // Sleep a little to ensure that we transition to Prospective
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();

        assertTrue(context.client.quorum().isProspective());

        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(epoch, 0, 0);
        assertEquals(1, voteRequests.size());

        // Simulate remote node not supporting PreVote and responding with Vote response with version 0
        context.deliverResponse(
            voteRequests.get(0).correlationId(),
            voteRequests.get(0).destination(),
            context.voteResponse(false, OptionalInt.empty(), epoch, (short) 0)
        );
        context.client.poll();

        // Local should transition to Candidate since it sees a remote node does not support PreVote. It can tell this
        // from responses with PreVote field set to false
        assertEquals(epoch + 1, context.currentEpoch());
        context.client.quorum().isCandidate();
    }

    @Test
    public void testProspectiveReceivesBeginQuorumRequest() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey leader = replicaKey(localId + 1, true);
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, local.directoryId().get())
            .withUnknownLeader(epoch)
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(local, leader))))
            .build();

        context.assertUnknownLeader(epoch);

        // Sleep a little to ensure that we transition to Prospective
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();

        assertTrue(context.client.quorum().isProspective());

        context.deliverRequest(context.beginEpochRequest(epoch, leader.id()));
        context.client.poll();

        assertTrue(context.client.quorum().isFollower());
        context.assertElectedLeader(epoch, leader.id());
    }

    @Test
    public void testProspectiveSendsFetchRequests() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey otherNode = replicaKey(localId + 1, true);
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, local.directoryId().get())
            .withUnknownLeader(epoch)
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(VoterSetTest.voterSet(Stream.of(local, otherNode))))
            .build();
        context.assertUnknownLeader(epoch);

        // Sleep a little to ensure that we transition to Prospective
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();
        assertTrue(context.client.quorum().isProspective());
        RaftRequest.Outbound voteRequest = context.assertSentVoteRequest(epoch, 0, 0L, 1);

        // If election timeout expires, we should start backing off and sending fetch requests
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.client.poll();
        assertTrue(context.client.quorum().isProspective());
        context.assertSentFetchRequest(epoch, 0, 0);

        context.time.sleep(context.electionBackoffMaxMs);

        // After the backoff, we will transition back to Prospective and continue sending PreVote requests
        context.pollUntilRequest();
        voteRequest = context.assertSentVoteRequest(epoch, 0, 0L, 1);

        // If we receive enough rejected votes, we also immediately start backing off and follow with fetch requests
        context.deliverResponse(
            voteRequest.correlationId(),
            voteRequest.destination(),
            context.voteResponse(false, OptionalInt.empty(), epoch, true));
        // handle vote response and update backoff timer
        context.client.poll();
        // send fetch request while in backoff
        context.client.poll();

        assertTrue(context.client.quorum().isProspective());
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest(epoch, 0, 0);
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, -1, MemoryRecords.EMPTY, -1, Errors.BROKER_NOT_AVAILABLE));
        context.client.poll();

        // We continue sending fetch requests until backoff timer expires
        context.time.sleep(context.electionBackoffMaxMs / 2);
        context.client.poll();
        assertTrue(context.client.quorum().isProspective());
        context.assertSentFetchRequest(epoch, 0, 0);
    }

    @Test
    public void testPreVoteResponseIncludesLeaderId() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey leader = replicaKey(local.id() + 1, true);
        ReplicaKey follower = replicaKey(local.id() + 2, true);
        int epoch = 5;

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, leader, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(epoch)
            .build();

        context.assertUnknownLeader(epoch);

        // Sleep a little to ensure that we transition to Prospective
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();

        assertTrue(context.client.quorum().isProspective());

        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(epoch, 0, 0);
        assertEquals(2, voteRequests.size());

        // Simulate PreVote response with granted=false and a leaderId
        VoteResponseData voteResponse1 = context.voteResponse(true, OptionalInt.of(leader.id()), epoch, false);
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
}
