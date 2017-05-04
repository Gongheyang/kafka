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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.CacheFlushListener;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;


class CachingSessionStore<K, AGG> extends WrappedStateStore.AbstractStateStore implements SessionStore<K, AGG>, CachedStateStore<Windowed<K>, AGG> {

    private final SessionStore<Bytes, byte[]> bytesStore;
    private final SessionKeySchema keySchema;
    private final Serde<K> keySerde;
    private final Serde<AGG> aggSerde;
    private String cacheName;
    private ThreadCache cache;
    private StateSerdes<K, AGG> serdes;
    private InternalProcessorContext context;
    private CacheFlushListener<Windowed<K>, AGG> flushListener;
    private String topic;

    CachingSessionStore(final SessionStore<Bytes, byte[]> bytesStore,
                        final Serde<K> keySerde,
                        final Serde<AGG> aggSerde) {
        super(bytesStore);
        this.bytesStore = bytesStore;
        this.keySerde = keySerde;
        this.aggSerde = aggSerde;
        this.keySchema = new SessionKeySchema();
    }

    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context, final StateStore root) {
        topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), root.name());
        bytesStore.init(context, root);
        initInternal((InternalProcessorContext) context);
    }

    @SuppressWarnings("unchecked")
    private void initInternal(final InternalProcessorContext context) {
        this.context = context;

        keySchema.init(topic);
        serdes = new StateSerdes<>(
            topic,
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            aggSerde == null ? (Serde<AGG>) context.valueSerde() : aggSerde);


        cacheName = context.taskId() + "-" + bytesStore.name();
        cache = context.getCache();
        cache.addDirtyEntryFlushListener(cacheName, new ThreadCache.DirtyEntryFlushListener() {
            @Override
            public void apply(final List<ThreadCache.DirtyEntry> entries) {
                for (ThreadCache.DirtyEntry entry : entries) {
                    putAndMaybeForward(entry, context);
                }
            }
        });
    }

    public KeyValueIterator<Windowed<K>, AGG> findSessions(final K key,
                                                           final long earliestSessionEndTime,
                                                           final long latestSessionStartTime) {
        validateStoreOpen();
        final Bytes binarySessionId = Bytes.wrap(keySerde.serializer().serialize(topic, key));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName,
                                                                                  keySchema.lowerRange(binarySessionId, earliestSessionEndTime),
                                                                                  keySchema.upperRange(binarySessionId, latestSessionStartTime));
        final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = bytesStore.findSessions(binarySessionId, earliestSessionEndTime, latestSessionStartTime);
        final HasNextCondition hasNextCondition = keySchema.hasNextCondition(binarySessionId,
                                                                             earliestSessionEndTime,
                                                                             latestSessionStartTime);
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, hasNextCondition);
        return new MergedSortedCacheSessionStoreIterator<>(filteredCacheIterator, storeIterator, serdes);
    }

    @Override
    public void remove(final Windowed<K> sessionKey) {
        validateStoreOpen();
        put(sessionKey, null);
    }

    @Override
    public void put(final Windowed<K> key, AGG value) {
        validateStoreOpen();
        final Bytes binaryKey = SessionKeySerde.toBinary(key, keySerde.serializer(), topic);
        final LRUCacheEntry entry = new LRUCacheEntry(serdes.rawValue(value), true, context.offset(),
                                                      key.window().end(), context.partition(), context.topic());
        cache.put(cacheName, binaryKey, entry);
    }

    @Override
    public KeyValueIterator<Windowed<K>, AGG> fetch(final K key) {
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry, final InternalProcessorContext context) {
        final Bytes binaryKey = entry.key();
        final RecordContext current = context.recordContext();
        context.setRecordContext(entry.recordContext());
        try {
            final Windowed<K> key = SessionKeySerde.from(binaryKey.get(), keySerde.deserializer(), topic);
            if (flushListener != null) {
                final AGG newValue = serdes.valueFrom(entry.newValue());
                final AGG oldValue = fetchPrevious(key);
                if (!(newValue == null && oldValue == null)) {
                    flushListener.apply(key, newValue, oldValue);
                }
            }
            bytesStore.put(new Windowed<>(Bytes.wrap(serdes.rawKey(key.key())), key.window()), entry.newValue());
        } finally {
            context.setRecordContext(current);
        }
    }

    private AGG fetchPrevious(final Windowed<K> key) {
        try (final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = bytesStore
                .findSessions(Bytes.wrap(serdes.rawKey(key.key())), key.window().start(), key.window().end())) {
            if (!iterator.hasNext()) {
                return null;
            }
            return serdes.valueFrom(iterator.next().value);
        }
    }

    public void flush() {
        cache.flush(cacheName);
        bytesStore.flush();
    }

    public void close() {
        flush();
        cache.close(cacheName);
        bytesStore.close();
    }

    public void setFlushListener(CacheFlushListener<Windowed<K>, AGG> flushListener) {
        this.flushListener = flushListener;
    }

}
