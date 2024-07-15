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
package org.apache.kafka.tiered.storage.actions;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ExpectListOffsetsAction implements TieredStorageTestAction {

    private final int brokerId;
    private final TopicPartition partition;
    private final OffsetSpec spec;
    private EpochEntry expected;

    public ExpectListOffsetsAction(int brokerId,
                                   TopicPartition partition,
                                   OffsetSpec spec,
                                   EpochEntry expected) {
        this.brokerId = brokerId;
        this.partition = partition;
        this.spec = spec;
        this.expected = expected;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException {
        if (expected.epoch == Integer.MAX_VALUE) {
            // The leaderEpoch for the partition gets bumped/incremented when the partition gets reassigned (or)
            // a new leader gets elected. But, the leaderEpoch value is non-deterministic between the ZK and Kraft mode.
            // So, reading the leader-epoch value from the broker's LeaderEpochFileCache for the given offset.
            Optional<LeaderEpochFileCache> leaderEpochFileCache = context.leaderEpochFileCache(brokerId, partition);
            assertTrue(leaderEpochFileCache.isPresent());
            leaderEpochFileCache.get().epochForOffset(expected.startOffset).ifPresent(epoch ->
                expected = new EpochEntry(epoch, expected.startOffset)
            );
        }

        ListOffsetsResult.ListOffsetsResultInfo listOffsetsResult = context.admin()
                .listOffsets(Collections.singletonMap(partition, spec))
                .all()
                .get()
                .get(partition);
        assertEquals(expected.startOffset, listOffsetsResult.offset());
        if (expected.epoch != -1) {
            assertTrue(listOffsetsResult.leaderEpoch().isPresent());
            assertEquals(expected.epoch, listOffsetsResult.leaderEpoch().get());
        } else {
            assertFalse(listOffsetsResult.leaderEpoch().isPresent());
        }
    }

    @Override
    public void describe(PrintStream output) {
        output.printf("expect-list-offsets broker-id: %d, partition: %s, spec: %s, expected-epoch-and-offset: %s%n",
                brokerId, partition, spec, expected);
    }
}
