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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.raft.internals.ReplicaKey;
import org.apache.kafka.raft.internals.VoterSet;
import org.apache.kafka.raft.internals.VoterSetTest;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriterReaderTest;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;
import static org.apache.kafka.snapshot.Snapshots.BOOTSTRAP_SNAPSHOT_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientReconfigTest {

    @Test
    public void testLeaderWritesBootstrapRecords() throws Exception {
        ReplicaKey local = replicaKey(0, true);
        ReplicaKey follower = replicaKey(1, true);
        int epoch = 0;

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(epoch)
            .build();

        // check the bootstrap snapshot exists and contains the expected records
        assertEquals(BOOTSTRAP_SNAPSHOT_ID, context.log.latestSnapshotId().get());
        RawSnapshotReader snapshot = context.log.latestSnapshot().get();
        // TODO: This should not be a List of String
        List<String> expectedBootstrapRecords = new ArrayList<>();
        expectedBootstrapRecords.add(new SnapshotHeaderRecord()
            .setVersion((short) 0)
            .setLastContainedLogTimestamp(0).toString());
        expectedBootstrapRecords.add(new KRaftVersionRecord()
            .setVersion(ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
            .setKRaftVersion((short) 1).toString());
        expectedBootstrapRecords.add(voters.toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION).toString());
        expectedBootstrapRecords.add(new SnapshotFooterRecord().setVersion((short) 0).toString());
        SnapshotWriterReaderTest.assertControlSnapshot(
            Collections.singletonList(expectedBootstrapRecords),
            snapshot
        );

        // check if leader writes 3 bootstrap records to log
        context.becomeLeader();

        // TODO: Fix this. This doesn't look correct
        Records records = context.log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());
        Iterator<Record> recordIterator = batch.iterator();
        Record record = recordIterator.next();
        RaftClientTestContext.verifyLeaderChangeMessage(
            local.id(),
            Arrays.asList(local.id(), follower.id()),
            Arrays.asList(local.id(), follower.id()),
            record.key(),
            record.value()
        );
        record = recordIterator.next();
        verifyKRaftVersionRecord((short) 1, record.key(), record.value());
        record = recordIterator.next();
        verifyVotersRecord(voters.voterIds(), record.key(), record.value());

        // check that leader does not respond with bootstrap snapshot id when follower fetches offset 0
        epoch = context.currentEpoch();
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                follower,
                0,
                0,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // check we receive error if follower requests bootstrap snapshot from the leader
        context.deliverRequest(
            KafkaRaftClientSnapshotTest.fetchSnapshotRequest(
                context.metadataPartition,
                epoch,
                BOOTSTRAP_SNAPSHOT_ID,
                Integer.MAX_VALUE,
                0
            )
        );
        context.pollUntilResponse();
        FetchSnapshotResponseData.PartitionSnapshot response = context
            .assertSentFetchSnapshotResponse(context.metadataPartition)
            .get();
        assertEquals(Errors.SNAPSHOT_NOT_FOUND, Errors.forCode(response.errorCode()));
    }

    @Test
    public void testLeaderDoesNotBootstrapRecordsWithKraftVersion0() throws Exception {
        ReplicaKey local = replicaKey(0, true);
        ReplicaKey follower = replicaKey(1, true);
        int epoch = 0;

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withStaticVoters(voters.voterIds())
            .withBootstrapSnapshot(Optional.empty())
            .withUnknownLeader(epoch)
            .build();

        // check the bootstrap snapshot exists but is empty
        assertEquals(BOOTSTRAP_SNAPSHOT_ID, context.log.latestSnapshotId().get());
        RawSnapshotReader snapshot = context.log.latestSnapshot().get();
        List<String> expectedBootstrapRecords = Collections.emptyList();
        SnapshotWriterReaderTest.assertControlSnapshot(
            Collections.singletonList(expectedBootstrapRecords),
            snapshot
        );

        // check leader does not write bootstrap records to log
        context.becomeLeader();
        Records records = context.log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());
        Iterator<Record> recordIterator = batch.iterator();
        Record record = recordIterator.next();
        RaftClientTestContext.verifyLeaderChangeMessage(
            local.id(),
            Arrays.asList(local.id(), follower.id()),
            Arrays.asList(local.id(), follower.id()),
            record.key(),
            record.value()
        );
        assertFalse(recordIterator.hasNext());
    }

    @Test
    public void testFollowerDoesNotRequestLeaderBootstrapSnapshot() throws Exception {
        ReplicaKey local = replicaKey(0, true);
        ReplicaKey leader = replicaKey(1, true);
        int epoch = 1;

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, leader));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withStaticVoters(voters.voterIds())
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, leader.id())
            .build();

        // check that follower will send fetch request to leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        // check if leader response were to contain bootstrap snapshot id, follower would not send fetch snapshot request
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leader.id(), BOOTSTRAP_SNAPSHOT_ID, 0)
        );
        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    @Test
    public void testFollowerReadsKRaftBootstrapRecords() throws Exception {
        ReplicaKey local = replicaKey(0, true);
        ReplicaKey leader = replicaKey(1, true);
        ReplicaKey follower = replicaKey(2, true);
        VoterSet voterSet = VoterSetTest.voterSet(Stream.of(local, leader));
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voterSet))
            .withElectedLeader(epoch, leader.id())
            .build();

        // check that follower will send fetch request to leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        // check that before receiving bootstrap records from leader, follower is not in the voter set
        assertFalse(context.client.quorum().isVoter(follower));

        // leader sends batch with bootstrap records
        VoterSet leadersVoterSet = VoterSetTest.voterSet(
            Stream.concat(voterSet.voterKeys().stream(), Stream.of(follower))
        );
        ByteBuffer buffer = ByteBuffer.allocate(128);
        try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
            buffer,
            RecordBatch.CURRENT_MAGIC_VALUE,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            0,
            0,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE,
            false,
            true,
            epoch,
            buffer.capacity())
        ) {
            builder.appendKRaftVersionMessage(
                0,
                new KRaftVersionRecord()
                    .setVersion(ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
                    .setKRaftVersion((short) 1)
            );
            builder.appendVotersMessage(
                0,
                leadersVoterSet.toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION)
            );
            MemoryRecords leaderRecords = builder.build();
            context.deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                context.fetchResponse(epoch, leader.id(), leaderRecords, 0, Errors.NONE));
        }

        // follower applies the bootstrap records, registering follower2 as a new voter
        context.client.poll();
        assertTrue(context.client.quorum().isVoter(follower));
    }

    // TODO: Not sure this is really needed
    private static void verifyVotersRecord(
        Set<Integer> expectedVoterIds,
        ByteBuffer recordKey,
        ByteBuffer recordValue
    ) {
        assertEquals(ControlRecordType.KRAFT_VOTERS, ControlRecordType.parse(recordKey));
        VotersRecord votersRecord = ControlRecordUtils.deserializeVotersRecord(recordValue);
        assertEquals(expectedVoterIds, votersRecord.voters().stream().map(VotersRecord.Voter::voterId).collect(Collectors.toSet()));
    }

    // TODO: Not sure this is really needed
    private static void verifyKRaftVersionRecord(
        short expectedKRaftVersion,
        ByteBuffer recordKey,
        ByteBuffer recordValue
    ) {
        assertEquals(ControlRecordType.KRAFT_VERSION, ControlRecordType.parse(recordKey));
        KRaftVersionRecord kRaftVersionRecord = ControlRecordUtils.deserializeKRaftVersionRecord(recordValue);
        assertEquals(expectedKRaftVersion, kRaftVersionRecord.kRaftVersion());
    }
}
