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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.AbstractNotifyingBatchingRestoreCallback;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RocksDBSegmentedBytesStore implements SegmentedBytesStore {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBSegmentedBytesStore.class);
    private final String name;
    private final Segments segments;
    private final KeySchema keySchema;
    private InternalProcessorContext context;
    private volatile boolean open;

    RocksDBSegmentedBytesStore(final String name,
                               final long retention,
                               final long segmentInterval,
                               final KeySchema keySchema) {
        this.name = name;
        this.keySchema = keySchema;
        this.segments = new Segments(name, retention, segmentInterval);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes key, final long from, final long to) {
        final List<Segment> searchSpace = keySchema.segmentsToSearch(segments, from, to);

        final Bytes binaryFrom = keySchema.lowerRangeFixedSize(key, from);
        final Bytes binaryTo = keySchema.upperRangeFixedSize(key, to);

        return new SegmentIterator(searchSpace.iterator(),
                                   keySchema.hasNextCondition(key, key, from, to),
                                   binaryFrom, binaryTo);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> fetch(final Bytes keyFrom, Bytes keyTo, final long from, final long to) {
        final List<Segment> searchSpace = keySchema.segmentsToSearch(segments, from, to);

        final Bytes binaryFrom = keySchema.lowerRange(keyFrom, from);
        final Bytes binaryTo = keySchema.upperRange(keyTo, to);

        return new SegmentIterator(searchSpace.iterator(),
                                   keySchema.hasNextCondition(keyFrom, keyTo, from, to),
                                   binaryFrom, binaryTo);
    }
    
    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        
        final List<Segment> searchSpace = segments.allSegments();
        
        return new SegmentIterator(searchSpace.iterator(),
                                   keySchema.hasNextCondition(null, null, 0, Long.MAX_VALUE),
                                   null, null);
    }
    
    @Override
    public KeyValueIterator<Bytes, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        final List<Segment> searchSpace = segments.segments(timeFrom, timeTo);
        
        return new SegmentIterator(searchSpace.iterator(),
                                   keySchema.hasNextCondition(null, null, timeFrom, timeTo),
                                   null, null);
    }
    
    @Override
    public void remove(final Bytes key) {
        final Segment segment = segments.getSegmentForTimestamp(keySchema.segmentTimestamp(key));
        if (segment == null) {
            return;
        }
        segment.delete(key);
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        final long segmentId = segments.segmentId(keySchema.segmentTimestamp(key));
        final Segment segment = segments.getOrCreateSegmentIfLive(segmentId, context);
        if (segment == null) {
            LOG.debug("Skipping record for expired segment.");
        } else {
            segment.put(key, value);
        }
    }

    @Override
    public byte[] get(final Bytes key) {
        final Segment segment = segments.getSegmentForTimestamp(keySchema.segmentTimestamp(key));
        if (segment == null) {
            return null;
        }
        return segment.get(key);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        this.context = (InternalProcessorContext) context;

        keySchema.init(ProcessorStateManager.storeChangelogTopic(context.applicationId(), root.name()));

        segments.openExisting(this.context);

        // register and possibly restore the state from the logs
        context.register(root, new RocksDBSegmentsBatchingRestoreCallback());

        open = true;
    }

    @Override
    public void flush() {
        segments.flush();
    }

    @Override
    public void close() {
        open = false;
        segments.close();
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    // Visible for testing
    List<Segment> getSegments() {
        return segments.allSegments();
    }

    // Visible for testing
    void restoreAllInternal(final Collection<KeyValue<byte[], byte[]>> records) {
        try {
            final Map<Segment, WriteBatch> writeBatchMap = getWriteBatches(records);
            for (Map.Entry<Segment, WriteBatch> entry: writeBatchMap.entrySet()) {
                final Segment segment = entry.getKey();
                final WriteBatch batch = entry.getValue();
                segment.write(batch);
            }
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error restoring batch to store " + this.name, e);
        }
    }

    // Visible for testing
    Map<Segment, WriteBatch> getWriteBatches(final Collection<KeyValue<byte[], byte[]>> records) {
        final Map<Segment, WriteBatch> writeBatchMap = new HashMap<>();
        for (final KeyValue<byte[], byte[]> record : records) {
            final long segmentId = segments.segmentId(keySchema.segmentTimestamp(Bytes.wrap(record.key)));
            final Segment segment = segments.getOrCreateSegmentIfLive(segmentId, context);
            if (segment != null) {
                final WriteBatch batch = writeBatchMap.getOrDefault(segment, new WriteBatch());
                if (record.value == null) {
                    batch.remove(record.key);
                } else {
                    batch.put(record.key, record.value);
                }
            }
        }
        return writeBatchMap;
    }

    private void toggleForBulkLoading(final boolean prepareForBulkload) {
        for (final Segment segment: segments.allSegments()) {
            segment.toggleDbForBulkLoading(prepareForBulkload);
        }
    }

    private class RocksDBSegmentsBatchingRestoreCallback extends AbstractNotifyingBatchingRestoreCallback {

        @Override
        public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
            restoreAllInternal(records);
        }

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
            toggleForBulkLoading(true);
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestored) {
            toggleForBulkLoading(false);
        }
    }
}
