/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.jmh.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueLoggedStore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An in-memory LRU cache store based on HashSet and HashMap.
 *
 * This class has been modified from the original
 * class to enable testing of caching objects by memory.
 *
 *  * Note that the use of array-typed keys is discouraged because they result in incorrect ordering behavior.
 * If you intend to work on byte arrays as key, for example, you may want to wrap them with the {@code Bytes} class,
 * i.e. use {@code RocksDBStore<Bytes, ...>} rather than {@code RocksDBStore<byte[], ...>}.
 *
 * @param <K> The key type
 * @param <V> The value type
 *
 * @see org.apache.kafka.streams.state.Stores#create(String)
 */
public class MockMemoryLRUCache<K, V> implements KeyValueStore<K, V> {

    public interface EldestEntryRemovalListener<K, V> {

        void apply(K key, V value);
    }
    private final Serde<K> keySerde;

    private final Serde<V> valueSerde;
    private String name;
    protected Map<K, V> map;
    protected Map<Bytes, byte[]> bytesMap;
    private StateSerdes<K, V> serdes;
    private volatile boolean open = true;
    private boolean isMaxCacheByMemory = false;
    private boolean isMeasureDeep = false;
    private boolean isCachingBytes = false;
    private int totalMemoryUsed = 0;
    private org.github.jamm.MemoryMeter memoryMeter = new org.github.jamm.MemoryMeter().withGuessing(org.github.jamm.MemoryMeter.Guess.FALLBACK_UNSAFE);

    protected EldestEntryRemovalListener<K, V> listener;
    protected EldestEntryRemovalListener<Bytes, byte[]> bytesCacheListener;

    // this is used for extended MemoryNavigableLRUCache only
    public MockMemoryLRUCache(Serde<K> keySerde, Serde<V> valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public MockMemoryLRUCache(String name, final int maxCacheSize, Serde<K> keySerde, Serde<V> valueSerde) {
        this(keySerde, valueSerde);
        this.name = name;

        // leave room for one extra entry to handle adding an entry before the oldest can be removed
        this.map = new LinkedHashMap<K, V>(maxCacheSize + 1, 1.01f, true) {
            private static final long serialVersionUID = 1L;

            //just for testing would limit cache size to ~ 2GB
            @Override
            public int size() {
                if (isMaxCacheByMemory) {
                    return totalMemoryUsed;
                }
                return super.size();
            }

            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                if (size() > maxCacheSize) {
                    K key = eldest.getKey();
                    if (isMaxCacheByMemory) {
                        if (isMeasureDeep) {
                            totalMemoryUsed -= memoryMeter.measureDeep(key);
                            totalMemoryUsed -= memoryMeter.measureDeep(eldest.getValue());
                        } else {
                            totalMemoryUsed -= memoryMeter.measure(key);
                            totalMemoryUsed -= memoryMeter.measure(eldest.getValue());
                        }
                    }
                    if (listener != null) listener.apply(key, eldest.getValue());
                    return true;
                }
                return false;
            }
        };

        this.bytesMap = new LinkedHashMap<Bytes, byte[]>(maxCacheSize + 1, 1.01f, true) {
            private static final long serialVersionUID = 1L;
            @Override
            protected boolean removeEldestEntry(Map.Entry<Bytes, byte[]> eldest) {
                if (size() > maxCacheSize) {
                    Bytes key = eldest.getKey();
                    if (bytesCacheListener != null) bytesCacheListener.apply(key, eldest.getValue());
                    return true;
                }
                return false;
            }

        };
    }

    public KeyValueStore<K, V> enableLogging() {
        return new InMemoryKeyValueLoggedStore<>(this.name, this, keySerde, valueSerde);
    }

    public MockMemoryLRUCache<K, V> whenEldestRemoved(EldestEntryRemovalListener<K, V> listener) {
        this.listener = listener;

        return this;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context, StateStore root) {
        // construct the serde
        this.serdes = new StateSerdes<>(name,
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        // register the store
        context.register(root, true, new StateRestoreCallback() {
            @Override
            public void restore(byte[] key, byte[] value) {
                // check value for null, to avoid  deserialization error.
                if (value == null) {
                    put(serdes.keyFrom(key), null);
                } else {
                    put(serdes.keyFrom(key), serdes.valueFrom(value));
                }
            }
        });
    }

    public void setIsCachingBytes(boolean cachingBytes) {
        this.isCachingBytes = cachingBytes;

    }

    public void setMaxCacheByMemory(boolean isByMemory) {
        isMaxCacheByMemory = isByMemory;
    }

    public void setIsMeasureDeep(boolean isMeasureDeep) {
        this.isMeasureDeep = isMeasureDeep;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized V get(K key) {
        if (isCachingBytes) {
            Bytes keyBytes = Bytes.wrap(keySerde.serializer().serialize(this.name, key));
            byte[] rawValue =  this.bytesMap.get(keyBytes);
            return valueSerde.deserializer().deserialize(this.name, rawValue);
        }
        return this.map.get(key);
    }

    @Override
    public synchronized void put(K key, V value) {
        if (isMaxCacheByMemory) {
            if (isMeasureDeep) {
                totalMemoryUsed += memoryMeter.measureDeep(key);
                totalMemoryUsed += memoryMeter.measureDeep(value);
            } else {
                totalMemoryUsed += memoryMeter.measure(key);
                totalMemoryUsed += memoryMeter.measure(value);
            }
            this.map.put(key, value);

        } else if (isCachingBytes) {
            Bytes keyBytes = Bytes.wrap(keySerde.serializer().serialize(this.name, key));
            this.bytesMap.put(keyBytes, valueSerde.serializer().serialize(this.name, value));
        } else {
            throw new IllegalStateException("must be caching by memory or bytes size");
        }
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        for (KeyValue<K, V> entry : entries)
            put(entry.key, entry.value);
    }

    @Override
    public synchronized V delete(K key) {
        V value = this.map.remove(key);
        return value;
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        throw new UnsupportedOperationException("MemoryLRUCache does not support range() function.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public KeyValueIterator<K, V> all() {
        throw new UnsupportedOperationException("MemoryLRUCache does not support all() function.");
    }

    @Override
    public long approximateNumEntries() {
        return this.map.size();
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
    }

    @Override
    public void close() {
        open = false;
    }

    public int size() {
        return this.map.size();
    }
}