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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT16;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.RECORDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProduceRequestTest {

    private final SimpleRecord simpleRecord = new SimpleRecord(System.currentTimeMillis(),
                                                               "key".getBytes(),
                                                               "value".getBytes());

    @Test
    public void shouldBeFlaggedAsTransactionalWhenTransactionalRecords() throws Exception {
        final MemoryRecords memoryRecords = MemoryRecords.withTransactionalRecords(0, CompressionType.NONE, 1L,
                (short) 1, 1, 1, simpleRecord);

        final ProduceRequest request = ProduceRequest.forCurrentMagic(new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                    new ProduceRequestData.TopicProduceData()
                        .setName("topic")
                        .setPartitionData(Collections.singletonList(
                            new ProduceRequestData.PartitionProduceData()
                                .setIndex(1)
                                .setRecords(memoryRecords)))).iterator()))
                .setAcks((short) -1)
                .setTimeoutMs(10)
                .setTransactionalId(null)).build();
        assertTrue(RequestUtils.hasTransactionalRecords(request));
    }

    @Test
    public void shouldNotBeFlaggedAsTransactionalWhenNoRecords() throws Exception {
        final ProduceRequest request = createNonIdempotentNonTransactionalRecords();
        assertFalse(RequestUtils.hasTransactionalRecords(request));
    }

    @Test
    public void shouldNotBeFlaggedAsIdempotentWhenRecordsNotIdempotent() throws Exception {
        final ProduceRequest request = createNonIdempotentNonTransactionalRecords();
        assertFalse(RequestUtils.hasTransactionalRecords(request));
    }

    @Test
    public void shouldBeFlaggedAsIdempotentWhenIdempotentRecords() throws Exception {
        final MemoryRecords memoryRecords = MemoryRecords.withIdempotentRecords(1, CompressionType.NONE, 1L,
                (short) 1, 1, 1, simpleRecord);
        final ProduceRequest request = ProduceRequest.forCurrentMagic(new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                    new ProduceRequestData.TopicProduceData()
                        .setName("topic")
                        .setPartitionData(Collections.singletonList(
                            new ProduceRequestData.PartitionProduceData()
                                .setIndex(1)
                                .setRecords(memoryRecords)))).iterator()))
                .setAcks((short) -1)
                .setTimeoutMs(10)
                .setTransactionalId(null)).build();
        assertTrue(RequestUtils.hasIdempotentRecords(request));
    }

    @Test
    public void testBuildWithOldMessageFormat() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        ProduceRequest.Builder requestBuilder = ProduceRequest.forMagic(RecordBatch.MAGIC_VALUE_V1,
            new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                    new ProduceRequestData.TopicProduceData().setName("test").setPartitionData(Collections.singletonList(
                        new ProduceRequestData.PartitionProduceData().setIndex(9).setRecords(builder.build()))))
                    .iterator()))
                .setAcks((short) 1)
                .setTimeoutMs(5000)
                .setTransactionalId(null));
        assertEquals(2, requestBuilder.oldestAllowedVersion());
        assertEquals(2, requestBuilder.latestAllowedVersion());
    }

    @Test
    public void testBuildWithCurrentMessageFormat() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        ProduceRequest.Builder requestBuilder = ProduceRequest.forMagic(RecordBatch.CURRENT_MAGIC_VALUE,
                new ProduceRequestData()
                        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                                new ProduceRequestData.TopicProduceData().setName("test").setPartitionData(Collections.singletonList(
                                        new ProduceRequestData.PartitionProduceData().setIndex(9).setRecords(builder.build()))))
                                .iterator()))
                        .setAcks((short) 1)
                        .setTimeoutMs(5000)
                        .setTransactionalId(null));
        assertEquals(3, requestBuilder.oldestAllowedVersion());
        assertEquals(ApiKeys.PRODUCE.latestVersion(), requestBuilder.latestAllowedVersion());
    }

    @Test
    public void testV3AndAboveShouldContainOnlyOneRecordBatch() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());
        builder.close();

        builder = MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.CREATE_TIME, 1L);
        builder.append(11L, "1".getBytes(), "b".getBytes());
        builder.append(12L, null, "c".getBytes());
        builder.close();

        buffer.flip();

        ProduceRequest.Builder requestBuilder = ProduceRequest.forCurrentMagic(new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                    new ProduceRequestData.TopicProduceData()
                        .setName("test")
                        .setPartitionData(Collections.singletonList(
                            new ProduceRequestData.PartitionProduceData()
                                .setIndex(0)
                                    .setRecords(MemoryRecords.readableRecords(buffer))))).iterator()))
                .setAcks((short) 1)
                .setTimeoutMs(5000)
                .setTransactionalId(null));
        assertThrowsInvalidRecordExceptionForAllVersions(requestBuilder);
    }

    @Test
    public void testV3AndAboveCannotHaveNoRecordBatches() {
        ProduceRequest.Builder requestBuilder = ProduceRequest.forCurrentMagic(new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                        new ProduceRequestData.TopicProduceData()
                                .setName("test")
                                .setPartitionData(Collections.singletonList(
                                        new ProduceRequestData.PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(MemoryRecords.EMPTY)))).iterator()))
                .setAcks((short) 1)
                .setTimeoutMs(5000)
                .setTransactionalId(null));
        assertThrowsInvalidRecordExceptionForAllVersions(requestBuilder);
    }

    @Test
    public void testV3AndAboveCannotUseMagicV0() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V0, CompressionType.NONE,
                TimestampType.NO_TIMESTAMP_TYPE, 0L);
        builder.append(10L, null, "a".getBytes());

        ProduceRequest.Builder requestBuilder = ProduceRequest.forCurrentMagic(new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                        new ProduceRequestData.TopicProduceData()
                                .setName("test")
                                .setPartitionData(Collections.singletonList(
                                        new ProduceRequestData.PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(builder.build())))).iterator()))
                .setAcks((short) 1)
                .setTimeoutMs(5000)
                .setTransactionalId(null));
        assertThrowsInvalidRecordExceptionForAllVersions(requestBuilder);
    }

    @Test
    public void testV3AndAboveCannotUseMagicV1() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());

        ProduceRequest.Builder requestBuilder = ProduceRequest.forCurrentMagic(new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                        new ProduceRequestData.TopicProduceData()
                                .setName("test")
                                .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
                                        .setIndex(0)
                                        .setRecords(builder.build()))))
                        .iterator()))
                .setAcks((short) 1)
                .setTimeoutMs(5000)
                .setTransactionalId(null));
        assertThrowsInvalidRecordExceptionForAllVersions(requestBuilder);
    }

    @Test
    public void testV6AndBelowCannotUseZStdCompression() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.ZSTD,
            TimestampType.CREATE_TIME, 0L);
        builder.append(10L, null, "a".getBytes());

        ProduceRequestData produceData = new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                    new ProduceRequestData.TopicProduceData()
                        .setName("test")
                        .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
                            .setIndex(0)
                            .setRecords(builder.build()))))
                    .iterator()))
                .setAcks((short) 1)
                .setTimeoutMs(1000)
                .setTransactionalId(null);
        // Can't create ProduceRequest instance with version within [3, 7)
        for (short version = 3; version < 7; version++) {

            ProduceRequest.Builder requestBuilder = new ProduceRequest.Builder(version, version, produceData);
            assertThrowsInvalidRecordExceptionForAllVersions(requestBuilder);
        }

        // Works fine with current version (>= 7)
        ProduceRequest.forCurrentMagic(produceData);
    }

    @Test
    public void testMixedTransactionalData() {
        final long producerId = 15L;
        final short producerEpoch = 5;
        final int sequence = 10;
        final String transactionalId = "txnlId";

        final MemoryRecords nonTxnRecords = MemoryRecords.withRecords(CompressionType.NONE,
                new SimpleRecord("foo".getBytes()));
        final MemoryRecords txnRecords = MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId,
                producerEpoch, sequence, new SimpleRecord("bar".getBytes()));

        ProduceRequest.Builder builder = ProduceRequest.forMagic(RecordBatch.CURRENT_MAGIC_VALUE,
                new ProduceRequestData()
                        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Arrays.asList(
                                new ProduceRequestData.TopicProduceData().setName("foo").setPartitionData(Collections.singletonList(
                                        new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(txnRecords))),
                                new ProduceRequestData.TopicProduceData().setName("foo").setPartitionData(Collections.singletonList(
                                        new ProduceRequestData.PartitionProduceData().setIndex(1).setRecords(nonTxnRecords))))
                                .iterator()))
                        .setAcks((short) 1)
                        .setTimeoutMs(5000)
                        .setTransactionalId(null));

        final ProduceRequest request = builder.build();
        assertTrue(RequestUtils.hasTransactionalRecords(request));
        assertTrue(RequestUtils.hasIdempotentRecords(request));
    }

    @Test
    public void testMixedIdempotentData() {
        final long producerId = 15L;
        final short producerEpoch = 5;
        final int sequence = 10;

        final MemoryRecords nonTxnRecords = MemoryRecords.withRecords(CompressionType.NONE,
                new SimpleRecord("foo".getBytes()));
        final MemoryRecords txnRecords = MemoryRecords.withIdempotentRecords(CompressionType.NONE, producerId,
                producerEpoch, sequence, new SimpleRecord("bar".getBytes()));

        ProduceRequest.Builder builder = ProduceRequest.forMagic(RecordVersion.current().value,
                new ProduceRequestData()
                        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Arrays.asList(
                                new ProduceRequestData.TopicProduceData().setName("foo").setPartitionData(Collections.singletonList(
                                        new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(txnRecords))),
                                new ProduceRequestData.TopicProduceData().setName("foo").setPartitionData(Collections.singletonList(
                                        new ProduceRequestData.PartitionProduceData().setIndex(1).setRecords(nonTxnRecords))))
                                .iterator()))
                        .setAcks((short) -1)
                        .setTimeoutMs(5000)
                        .setTransactionalId(null));

        final ProduceRequest request = builder.build();
        assertFalse(RequestUtils.hasTransactionalRecords(request));
        assertTrue(RequestUtils.hasIdempotentRecords(request));
    }

    private void assertThrowsInvalidRecordExceptionForAllVersions(ProduceRequest.Builder builder) {
        for (short version = builder.oldestAllowedVersion(); version < builder.latestAllowedVersion(); version++) {
            assertThrowsInvalidRecordException(builder, version);
        }
    }

    private void assertThrowsInvalidRecordException(ProduceRequest.Builder builder, short version) {
        try {
            builder.build(version).toStruct();
            fail("Builder did not raise " + InvalidRecordException.class.getName() + " as expected");
        } catch (RuntimeException e) {
            assertTrue("Unexpected exception type " + e.getClass().getName(),
                    InvalidRecordException.class.isAssignableFrom(e.getClass()));
        }
    }

    private ProduceRequest createNonIdempotentNonTransactionalRecords() {
        return ProduceRequest.forCurrentMagic(new ProduceRequestData()
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                        new ProduceRequestData.TopicProduceData()
                                .setName("topic")
                                .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
                                        .setIndex(1)
                                        .setRecords(MemoryRecords.withRecords(CompressionType.NONE, simpleRecord)))))
                        .iterator()))
                .setAcks((short) -1)
                .setTimeoutMs(10)
                .setTransactionalId(null)).build();
    }

    /**
     * the schema in this test is from previous code and the automatic protocol should be compatible to previous schema.
     */
    @Test
    public void testCompatibility() {
        String acksKeyName = "acks";
        String timeoutKeyName = "timeout";
        String topicDataKeyName = "topic_data";
        String partitionDataKeyName = "data";
        String recordSetKeyName = "record_set";
        Schema topicProduceData0 = new Schema(TOPIC_NAME,
                new Field(partitionDataKeyName, new ArrayOf(new Schema(PARTITION_ID, new Field(recordSetKeyName, RECORDS)))));
        Schema produceRequestV0 = new Schema(
                new Field(acksKeyName, INT16),
                new Field(timeoutKeyName, INT32),
                new Field(topicDataKeyName, new ArrayOf(topicProduceData0)));
        Schema produceRequestV1 = produceRequestV0;
        Schema produceRequestV2 = produceRequestV1;
        Schema produceRequestV3 = new Schema(
                new Field.NullableStr("transactional_id", "The transactional id or null if the producer is not transactional"),
                new Field(acksKeyName, INT16),
                new Field(timeoutKeyName, INT32),
                new Field(topicDataKeyName, new ArrayOf(topicProduceData0)));
        Schema produceRequestV4 = produceRequestV3;
        Schema produceRequestV5 = produceRequestV4;
        Schema produceRequestV6 = produceRequestV5;
        Schema produceRequestV7 = produceRequestV6;
        Schema produceRequestV8 = produceRequestV7;
        Schema[] schemaVersions = new Schema[] {
            produceRequestV0, produceRequestV1, produceRequestV2,
            produceRequestV3, produceRequestV4, produceRequestV5,
            produceRequestV6, produceRequestV7, produceRequestV8
        };
        int schemaVersion = 0;
        for (Schema previousSchema : schemaVersions) {
            SchemaTestUtils.assertEquals(previousSchema, ProduceRequestData.SCHEMAS[schemaVersion++]);
        }
    }
}
