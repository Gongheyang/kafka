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
package org.apache.kafka.connect.runtime;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.connect.runtime.SubmittedRecords.SubmittedRecord;
import static org.junit.Assert.assertEquals;

public class SubmittedRecordsTest {

    private static final Map<String, Object> PARTITION1 = Collections.singletonMap("subreddit", "apachekafka");
    private static final Map<String, Object> PARTITION2 = Collections.singletonMap("subreddit", "pcj");
    private static final Map<String, Object> PARTITION3 = Collections.singletonMap("subreddit", "asdfqweoicus");

    private AtomicInteger offset;

    SubmittedRecords submittedRecords;

    @Before
    public void setup() {
        submittedRecords = new SubmittedRecords();
        offset = new AtomicInteger();
    }

    @Test
    public void testNoRecords() {
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());
    }

    @Test
    public void testNoCommittedRecords() {
        for (int i = 0; i < 3; i++) {
            for (Map<String, Object> partition : Arrays.asList(PARTITION1, PARTITION2, PARTITION3)) {
                submittedRecords.submit(partition, newOffset());
            }
        }
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());
    }

    @Test
    public void testSingleAck() {
        Map<String, Object> offset = newOffset();

        SubmittedRecord submittedRecord = submittedRecords.submit(PARTITION1, offset);
        // Record has been submitted but not yet acked; cannot commit offsets for it yet
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());

        submittedRecord.ack();
        // Record has been acked; can commit offsets for it
        assertEquals(Collections.singletonMap(PARTITION1, offset), submittedRecords.committableOffsets());

        // Old offsets should be wiped
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());
    }

    @Test
    public void testMultipleAcksAcrossMultiplePartitions() {
        Map<String, Object> partition1Offset1 = newOffset();
        Map<String, Object> partition1Offset2 = newOffset();
        Map<String, Object> partition2Offset1 = newOffset();
        Map<String, Object> partition2Offset2 = newOffset();

        SubmittedRecord partition1Record1 = submittedRecords.submit(PARTITION1, partition1Offset1);
        SubmittedRecord partition1Record2 = submittedRecords.submit(PARTITION1, partition1Offset2);
        SubmittedRecord partition2Record1 = submittedRecords.submit(PARTITION2, partition2Offset1);
        SubmittedRecord partition2Record2 = submittedRecords.submit(PARTITION2, partition2Offset2);

        // No records ack'd yet; can't commit any offsets
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());

        partition1Record2.ack();
        // One record has been ack'd, but a record that comes before it and corresponds to the same source partition hasn't been
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());

        partition2Record1.ack();
        // We can commit the first offset for the second partition
        assertEquals(Collections.singletonMap(PARTITION2, partition2Offset1), submittedRecords.committableOffsets());

        // No new offsets to commit
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());

        partition1Record1.ack();
        partition2Record2.ack();
        // We can commit new offsets for both partitions now
        Map<Map<String, Object>, Map<String, Object>> expectedOffsets = new HashMap<>();
        expectedOffsets.put(PARTITION1, partition1Offset2);
        expectedOffsets.put(PARTITION2, partition2Offset2);
        assertEquals(expectedOffsets, submittedRecords.committableOffsets());

        // No new offsets to commit
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());
    }

    @Test
    public void testRemove() {
        SubmittedRecord submittedRecord = submittedRecords.submit(PARTITION1, newOffset());
        submittedRecords.remove(submittedRecord);

        // Even if SubmittedRecords::remove is broken, we haven't ack'd anything yet, so there should be no committable offsets
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());

        submittedRecord.ack();
        // Even though the record has somehow been acknowledged, it should not be counted when collecting committable offsets
        assertEquals(Collections.emptyMap(), submittedRecords.committableOffsets());
    }

    @Test
    public void testNullPartitionAndOffset() {
        SubmittedRecord submittedRecord = submittedRecords.submit(null, null);
        submittedRecord.ack();
        assertEquals(Collections.singletonMap(null, null), submittedRecords.committableOffsets());
    }

    private Map<String, Object> newOffset() {
        return Collections.singletonMap("timestamp", offset.getAndIncrement());
    }
}
