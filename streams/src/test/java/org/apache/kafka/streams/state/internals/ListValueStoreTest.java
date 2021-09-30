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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static java.util.Arrays.asList;
import static org.apache.kafka.test.StreamsTestUtils.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ListValueStoreTest {
    private enum StoreType { InMemory, RocksDB }

    private final StoreType storeType;
    private KeyValueStore<Integer, String> listStore;

    final File baseDir = TestUtils.tempDirectory("test");

    public ListValueStoreTest(final StoreType type) {
        this.storeType = type;
    }

    @Parameterized.Parameters(name = "store type = {0}")
    public static Collection<Object[]> data() {
        final List<Object[]> values = new ArrayList<>();
        for (final StoreType type : Arrays.asList(StoreType.InMemory, StoreType.RocksDB)) {
            values.add(new Object[]{type});
        }
        return values;
    }

    @Before
    public void setup() {
        listStore = buildStore(Serdes.Integer(), Serdes.String());

        final MockRecordCollector recordCollector = new MockRecordCollector();
        final InternalMockProcessorContext<Integer, String> context = new InternalMockProcessorContext<>(
            baseDir,
            Serdes.String(),
            Serdes.Integer(),
            recordCollector,
            new ThreadCache(
                new LogContext("testCache"),
                0,
                new MockStreamsMetrics(new Metrics())));
        context.setTime(1L);

        listStore.init((StateStoreContext) context, listStore);
    }

    @After
    public void after() {
        listStore.close();
    }

    <K, V> KeyValueStore<K, V> buildStore(final Serde<K> keySerde,
                                          final Serde<V> valueSerde) {
        return new ListValueStoreBuilder<>(
            storeType == StoreType.RocksDB ? Stores.persistentKeyValueStore("rocksDB list store")
                //: Stores.inMemoryKeyValueStore("in-memory list store"),
                : new KeyValueBytesStoreSupplier() {
                @Override
                public String name() {
                    return "rocksDB list store";
                }

                @Override
                public KeyValueStore<Bytes, byte[]> get() {
                    // do not copy of range since it would not be used for IQ
                    return new InMemoryKeyValueStore("rocksDB list store", false);
                }

                @Override
                public String metricsScope() {
                    return "in-memory";
                }
            },
            keySerde,
            valueSerde,
            Time.SYSTEM)
            .build();
    }

    @Test
    public void shouldGetAll() {
        listStore.put(0, "zero");
        // should retain duplicates
        listStore.put(0, "zero again");
        listStore.put(1, "one");
        listStore.put(2, "two");

        final KeyValue<Integer, String> zero = KeyValue.pair(0, "zero");
        final KeyValue<Integer, String> zeroAgain = KeyValue.pair(0, "zero again");
        final KeyValue<Integer, String> one = KeyValue.pair(1, "one");
        final KeyValue<Integer, String> two = KeyValue.pair(2, "two");

        assertEquals(
            asList(zero, zeroAgain, one, two),
            toList(listStore.all())
        );
    }

    @Test
    public void shouldGetAllNonDeletedRecords() {
        // Add some records
        listStore.put(0, "zero");
        listStore.put(1, "one");
        listStore.put(1, "one again");
        listStore.put(2, "two");
        listStore.put(3, "three");
        listStore.put(4, "four");

        // Delete some records
        listStore.putIfAbsent(1, null);
        listStore.putIfAbsent(3, null);
        listStore.putIfAbsent(5, null);

        // Only non-deleted records should appear in the all() iterator
        final KeyValue<Integer, String> zero = KeyValue.pair(0, "zero");
        final KeyValue<Integer, String> two = KeyValue.pair(2, "two");
        final KeyValue<Integer, String> four = KeyValue.pair(4, "four");

        assertEquals(
            asList(zero, two, four),
            toList(listStore.all())
        );
    }

    @Test
    public void shouldGetAllReturnTimestampOrderedRecords() {
        // Add some records in different order
        listStore.put(4, "four");
        listStore.put(0, "zero");
        listStore.put(2, "two1");
        listStore.put(3, "three");
        listStore.put(1, "one");

        // Add duplicates
        listStore.put(2, "two2");

        // Only non-deleted records should appear in the all() iterator
        final KeyValue<Integer, String> zero = KeyValue.pair(0, "zero");
        final KeyValue<Integer, String> one = KeyValue.pair(1, "one");
        final KeyValue<Integer, String> two1 = KeyValue.pair(2, "two1");
        final KeyValue<Integer, String> two2 = KeyValue.pair(2, "two2");
        final KeyValue<Integer, String> three = KeyValue.pair(3, "three");
        final KeyValue<Integer, String> four = KeyValue.pair(4, "four");

        assertEquals(
            asList(zero, one, two1, two2, three, four),
            toList(listStore.all())
        );
    }

    @Test
    public void shouldAllowDeleteWhileIterateRecords() {
        final Random rand = new Random();
        int count = 0;
        for (int i = 0; i < 10000; i++) {
            listStore.put(i, "zero" + i);
            count++;

            while (rand.nextBoolean()) {
                listStore.put(i, "zero" + i);
                count++;
            }
        }

        final int size = toList(listStore.all()).size();
        assertEquals(count, size);

        final KeyValueIterator<Integer, String> it = listStore.all();

        int prev = -1;
        int deleted = 0;
        int dupCount = 0;
        while (it.hasNext()) {
            final KeyValue<Integer, String> entry = it.next();

            if (prev != -1 && prev != entry.key) {
                if (rand.nextBoolean()) {
                    listStore.put(prev, null);
                    deleted += dupCount;
                }

                dupCount = 0;
            }

            dupCount++;
            prev = entry.key;
        }

        it.close();

        // A new all() iterator after a previous all() iterator was closed should not return deleted records.
        assertEquals(size - deleted, toList(listStore.all()).size());
    }

    @Test
    public void shouldNotReturnMoreDataWhenIteratorClosed() {
        listStore.put(0, "zero1");
        listStore.put(0, "zero2");
        listStore.put(1, "one");

        final KeyValueIterator<Integer, String> it = listStore.all();

        it.close();

        // A new all() iterator after a previous all() iterator was closed should not return deleted records.
        assertThrows(InvalidStateStoreException.class, it::next);
    }
}
