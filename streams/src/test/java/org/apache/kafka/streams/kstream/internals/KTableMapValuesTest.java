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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KTableMapValuesTest {

    final private Serde<String> stringSerde = Serdes.String();

    private KStreamTestDriver driver = null;
    private File stateDir = null;

    @After
    public void tearDown() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Before
    public void setUp() throws IOException {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    private void doTestKTable(final StreamsBuilder builder, final String topic1, final MockProcessorSupplier<String, Integer> proc2) {
        driver = new KStreamTestDriver(builder, stateDir, Serdes.String(), Serdes.String());

        driver.process(topic1, "A", "1");
        driver.process(topic1, "B", "2");
        driver.process(topic1, "C", "3");
        driver.process(topic1, "D", "4");
        driver.flushState();
        assertEquals(Utils.mkList("A:1", "B:2", "C:3", "D:4"), proc2.processed);
    }

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        KTable<String, String> table1 = builder.table(stringSerde, stringSerde, topic1, "anyStoreName");
        KTable<String, Integer> table2 = table1.mapValues(new ValueMapper<CharSequence, Integer>() {
            @Override
            public Integer apply(CharSequence value) {
                return value.charAt(0) - 48;
            }
        });

        MockProcessorSupplier<String, Integer> proc2 = new MockProcessorSupplier<>();
        table2.toStream().process(proc2);

        doTestKTable(builder, topic1, proc2);
    }

    @Test
    public void testQueryableKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        KTable<String, String> table1 = builder.table(stringSerde, stringSerde, topic1, "anyStoreName");
        KTable<String, Integer> table2 = table1.mapValues(new ValueMapper<CharSequence, Integer>() {
            @Override
            public Integer apply(CharSequence value) {
                return value.charAt(0) - 48;
            }
        }, Serdes.Integer(), "anyName");

        MockProcessorSupplier<String, Integer> proc2 = new MockProcessorSupplier<>();
        table2.toStream().process(proc2);

        doTestKTable(builder, topic1, proc2);
    }

    private void doTestValueGetter(final StreamsBuilder builder,
                                   final String topic1,
                                   final KTableImpl<String, String, String> table1,
                                   final KTableImpl<String, String, Integer> table2,
                                   final KTableImpl<String, Integer, Integer> table3,
                                   final KTableImpl<String, String, String> table4) {
        KTableValueGetterSupplier<String, String> getterSupplier1 = table1.valueGetterSupplier();
        KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();
        KTableValueGetterSupplier<String, Integer> getterSupplier3 = table3.valueGetterSupplier();
        KTableValueGetterSupplier<String, String> getterSupplier4 = table4.valueGetterSupplier();

        driver = new KStreamTestDriver(builder, stateDir, Serdes.String(), Serdes.String());
        KTableValueGetter<String, String> getter1 = getterSupplier1.get();
        getter1.init(driver.context());
        KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
        getter2.init(driver.context());
        KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();
        getter3.init(driver.context());
        KTableValueGetter<String, String> getter4 = getterSupplier4.get();
        getter4.init(driver.context());

        driver.process(topic1, "A", "01");
        driver.process(topic1, "B", "01");
        driver.process(topic1, "C", "01");
        driver.flushState();

        assertEquals("01", getter1.get("A"));
        assertEquals("01", getter1.get("B"));
        assertEquals("01", getter1.get("C"));

        assertEquals(new Integer(1), getter2.get("A"));
        assertEquals(new Integer(1), getter2.get("B"));
        assertEquals(new Integer(1), getter2.get("C"));

        assertNull(getter3.get("A"));
        assertNull(getter3.get("B"));
        assertNull(getter3.get("C"));

        assertEquals("01", getter4.get("A"));
        assertEquals("01", getter4.get("B"));
        assertEquals("01", getter4.get("C"));

        driver.process(topic1, "A", "02");
        driver.process(topic1, "B", "02");
        driver.flushState();

        assertEquals("02", getter1.get("A"));
        assertEquals("02", getter1.get("B"));
        assertEquals("01", getter1.get("C"));

        assertEquals(new Integer(2), getter2.get("A"));
        assertEquals(new Integer(2), getter2.get("B"));
        assertEquals(new Integer(1), getter2.get("C"));

        assertEquals(new Integer(2), getter3.get("A"));
        assertEquals(new Integer(2), getter3.get("B"));
        assertNull(getter3.get("C"));

        assertEquals("02", getter4.get("A"));
        assertEquals("02", getter4.get("B"));
        assertEquals("01", getter4.get("C"));

        driver.process(topic1, "A", "03");
        driver.flushState();

        assertEquals("03", getter1.get("A"));
        assertEquals("02", getter1.get("B"));
        assertEquals("01", getter1.get("C"));

        assertEquals(new Integer(3), getter2.get("A"));
        assertEquals(new Integer(2), getter2.get("B"));
        assertEquals(new Integer(1), getter2.get("C"));

        assertNull(getter3.get("A"));
        assertEquals(new Integer(2), getter3.get("B"));
        assertNull(getter3.get("C"));

        assertEquals("03", getter4.get("A"));
        assertEquals("02", getter4.get("B"));
        assertEquals("01", getter4.get("C"));

        driver.process(topic1, "A", null);
        driver.flushState();

        assertNull(getter1.get("A"));
        assertEquals("02", getter1.get("B"));
        assertEquals("01", getter1.get("C"));

        assertNull(getter2.get("A"));
        assertEquals(new Integer(2), getter2.get("B"));
        assertEquals(new Integer(1), getter2.get("C"));

        assertNull(getter3.get("A"));
        assertEquals(new Integer(2), getter3.get("B"));
        assertNull(getter3.get("C"));

        assertNull(getter4.get("A"));
        assertEquals("02", getter4.get("B"));
        assertEquals("01", getter4.get("C"));
    }

    @Test
    public void testValueGetter() throws IOException {
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";
        String topic2 = "topic2";
        String storeName1 = "storeName1";
        String storeName2 = "storeName2";

        KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(stringSerde, stringSerde, topic1, storeName1);
        KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(String value) {
                        return new Integer(value);
                    }
                });
        KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table2.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                });
        KTableImpl<String, String, String> table4 = (KTableImpl<String, String, String>)
                table1.through(stringSerde, stringSerde, topic2, storeName2);

        doTestValueGetter(builder, topic1, table1, table2, table3, table4);
    }

    @Test
    public void testQueryableValueGetter() throws IOException {
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";
        String topic2 = "topic2";
        String storeName1 = "storeName1";
        String storeName2 = "storeName2";

        KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(stringSerde, stringSerde, topic1, storeName1);
        KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
            new ValueMapper<String, Integer>() {
                @Override
                public Integer apply(String value) {
                    return new Integer(value);
                }
            }, Serdes.Integer(), "anyMapName");
        KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table2.filter(
            new Predicate<String, Integer>() {
                @Override
                public boolean test(String key, Integer value) {
                    return (value % 2) == 0;
                }
            }, "anyFilterName");
        KTableImpl<String, String, String> table4 = (KTableImpl<String, String, String>)
            table1.through(stringSerde, stringSerde, topic2, storeName2);

        doTestValueGetter(builder, topic1, table1, table2, table3, table4);
    }

    @Test
    public void testNotSendingOldValue() throws IOException {
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(stringSerde, stringSerde, topic1, "anyStoreName");
        KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(String value) {
                        return new Integer(value);
                    }
                });

        MockProcessorSupplier<String, Integer> proc = new MockProcessorSupplier<>();

        builder.build().addProcessor("proc", proc, table2.name);

        driver = new KStreamTestDriver(builder, stateDir, null, null);
        assertFalse(table1.sendingOldValueEnabled());
        assertFalse(table2.sendingOldValueEnabled());

        driver.process(topic1, "A", "01");
        driver.process(topic1, "B", "01");
        driver.process(topic1, "C", "01");
        driver.flushState();

        proc.checkAndClearProcessResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");

        driver.process(topic1, "A", "02");
        driver.process(topic1, "B", "02");
        driver.flushState();

        proc.checkAndClearProcessResult("A:(2<-null)", "B:(2<-null)");

        driver.process(topic1, "A", "03");
        driver.flushState();

        proc.checkAndClearProcessResult("A:(3<-null)");

        driver.process(topic1, "A", null);
        driver.flushState();

        proc.checkAndClearProcessResult("A:(null<-null)");
    }

    @Test
    public void testSendingOldValue() throws IOException {
        StreamsBuilder builder = new StreamsBuilder();

        String topic1 = "topic1";

        KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(stringSerde, stringSerde, topic1, "anyStoreName");
        KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(String value) {
                        return new Integer(value);
                    }
                });

        table2.enableSendingOldValues();

        MockProcessorSupplier<String, Integer> proc = new MockProcessorSupplier<>();

        builder.build().addProcessor("proc", proc, table2.name);

        driver = new KStreamTestDriver(builder, stateDir, null, null);
        assertTrue(table1.sendingOldValueEnabled());
        assertTrue(table2.sendingOldValueEnabled());

        driver.process(topic1, "A", "01");
        driver.process(topic1, "B", "01");
        driver.process(topic1, "C", "01");
        driver.flushState();

        proc.checkAndClearProcessResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");

        driver.process(topic1, "A", "02");
        driver.process(topic1, "B", "02");
        driver.flushState();

        proc.checkAndClearProcessResult("A:(2<-1)", "B:(2<-1)");

        driver.process(topic1, "A", "03");
        driver.flushState();

        proc.checkAndClearProcessResult("A:(3<-2)");

        driver.process(topic1, "A", null);
        driver.flushState();

        proc.checkAndClearProcessResult("A:(null<-3)");
    }
}
