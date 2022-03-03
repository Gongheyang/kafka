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

import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.StoreToProcessorContextAdapter;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;


public class RocksDBTimeOrderedWindowStore implements WindowStore<Bytes, byte[]> {

    private final boolean retainDuplicates;
    private final long windowSize;
    private final String name;
    private int seqnum = 0;

    private RocksDBTimeOrderedSegmentedBytesStore store;

    RocksDBTimeOrderedWindowStore(
        final RocksDBTimeOrderedSegmentedBytesStore store,
        final String name,
        final boolean retainDuplicates,
        final long windowSize
    ) {
        Objects.requireNonNull(store, "store is null");
        Objects.requireNonNull(name, "name is null");
        this.store = store;
        this.name = name;
        this.retainDuplicates = retainDuplicates;
        this.windowSize = windowSize;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        store.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        init(StoreToProcessorContextAdapter.adapt(context), root);
    }

    @Override
    public void flush() {
        store.flush();
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public boolean persistent() {
        return store.persistent();
    }

    @Override
    public boolean isOpen() {
        return store.isOpen();
    }

    @Override
    public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
        // Skip if value is null and duplicates are allowed since this delete is a no-op
        if (!(value == null && retainDuplicates)) {
            maybeUpdateSeqnumForDups();
            store.put(key, windowStartTimestamp, seqnum, value);
        }
    }

    @Override
    public byte[] fetch(final Bytes key, final long timestamp) {
        // TODO: check if some segments in index store can be purged
        return store.fetch(key, timestamp, seqnum);
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = store.fetch(key, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).valuesIterator();
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = store.backwardFetch(key, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).valuesIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo,
                                                           final long timeFrom,
                                                           final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = store.fetch(keyFrom, keyTo, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo,
                                                                   final long timeFrom,
                                                                   final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = store.backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = store.all();
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = store.backwardAll();
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = store.fetchAll(timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = store.backwardFetchAll(timeFrom, timeTo);
        return new WindowStoreIteratorWrapper(bytesIterator,
            windowSize,
            TimeFirstWindowKeySchema::extractStoreTimestamp,
            TimeFirstWindowKeySchema::fromStoreBytesKey).keyValueIterator();
    }

    private void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }
}
