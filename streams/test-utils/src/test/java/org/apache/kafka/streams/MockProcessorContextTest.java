package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MockProcessorContextTest {
    /**
     * Behavioral test demonstrating the use of the context for capturing forwarded values
     */
    @Test public void testForward() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testForward");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            @Override public void process(final String key, final Long value) {
                this.context().forward(key + value, key.length() + value);
            }
        };

        final MockProcessorContext context = new MockProcessorContext(config);

        processor.init(context);

        processor.process("foo", 5L);
        processor.process("barbaz", 50L);

        final Iterator<KeyValue> forwarded = context.forwarded().iterator();
        assertEquals(forwarded.next(), new KeyValue<>("foo5", 8L));
        assertEquals(forwarded.next(), new KeyValue<>("barbaz50", 56L));
        Assert.assertFalse(forwarded.hasNext());

        context.resetForwards();

        assertEquals(context.forwarded().size(), 0);
    }

    /**
     * Behavioral test demonstrating the use of the context for capturing forwarded values to specific children
     */
    @Test public void testForwardTo() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testForwardTo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            private int count = 0;

            @Override public void process(final String key, final Long value) {
                final To child = count % 2 == 0 ? To.child("george") : To.child("pete");
                this.context().forward(key + value, key.length() + value, child);
                count++;
            }
        };

        final MockProcessorContext context = new MockProcessorContext(config);

        processor.init(context);

        processor.process("foo", 5L);
        processor.process("barbaz", 50L);

        {
            final Iterator<KeyValue> forwarded = context.forwarded().iterator();
            assertEquals(forwarded.next(), new KeyValue<>("foo5", 8L));
            assertEquals(forwarded.next(), new KeyValue<>("barbaz50", 56L));
            Assert.assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<KeyValue> forwarded = context.forwarded("george").iterator();
            assertEquals(forwarded.next(), new KeyValue<>("foo5", 8L));
            Assert.assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<KeyValue> forwarded = context.forwarded("pete").iterator();
            assertEquals(forwarded.next(), new KeyValue<>("barbaz50", 56L));
            Assert.assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<KeyValue> forwarded = context.forwarded("steve").iterator();
            Assert.assertFalse(forwarded.hasNext());
        }

        context.resetForwards();

        assertEquals(context.forwarded().size(), 0);
    }

    /**
     * Behavioral test demonstrating the use of the context for capturing forwarded values to children by index (deprecated usage)
     */
    @Test public void testForwardToIndex() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testForwardTo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            private int count = 0;

            @Override public void process(final String key, final Long value) {
                this.context().forward(key + value, key.length() + value, count % 2);
                count++;
            }
        };

        final MockProcessorContext context = new MockProcessorContext(config);

        processor.init(context);

        processor.process("foo", 5L);
        processor.process("barbaz", 50L);

        {
            final Iterator<KeyValue> forwarded = context.forwarded().iterator();
            assertEquals(forwarded.next(), new KeyValue<>("foo5", 8L));
            assertEquals(forwarded.next(), new KeyValue<>("barbaz50", 56L));
            Assert.assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<KeyValue> forwarded = context.forwarded(0).iterator();
            assertEquals(forwarded.next(), new KeyValue<>("foo5", 8L));
            Assert.assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<KeyValue> forwarded = context.forwarded(1).iterator();
            assertEquals(forwarded.next(), new KeyValue<>("barbaz50", 56L));
            Assert.assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<KeyValue> forwarded = context.forwarded(2).iterator();
            Assert.assertFalse(forwarded.hasNext());
        }

        context.resetForwards();

        assertEquals(context.forwarded().size(), 0);
    }

    /**
     * Behavioral test demonstrating the use of the context for capturing commits
     */
    @Test public void testCommit() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testCommit");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            private int count = 0;

            @Override public void process(final String key, final Long value) {
                if (++count > 2) context().commit();
            }
        };

        final MockProcessorContext context = new MockProcessorContext(config);

        processor.init(context);

        processor.process("foo", 5L);
        processor.process("barbaz", 50L);

        Assert.assertFalse(context.committed());

        processor.process("foobar", 500L);

        Assert.assertTrue(context.committed());

        context.resetCommit();

        Assert.assertFalse(context.committed());
    }

    /**
     * Behavioral test demonstrating the use of state stores
     */
    @Test public void testState() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testForwardTo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            @Override public void process(final String key, final Long value) {
                //noinspection unchecked
                final KeyValueStore<String, Long> stateStore = (KeyValueStore<String, Long>) context().getStateStore("my-state");
                stateStore.put(key, (stateStore.get(key) == null ? 0 : stateStore.get(key)) + value);
                stateStore.put("all", (stateStore.get("all") == null ? 0 : stateStore.get("all")) + value);
            }
        };

        final MockProcessorContext context = new MockProcessorContext(config);
        final KeyValueStore<String, Long> store = new InMemoryKeyValueStore<>("my-state", Serdes.String(), Serdes.Long());
        context.register(store, false, null);

        processor.init(context);

        processor.process("foo", 5L);
        processor.process("bar", 50L);

        Assert.assertEquals(5L, (long) store.get("foo"));
        Assert.assertEquals(50L, (long) store.get("bar"));
        Assert.assertEquals(55L, (long) store.get("all"));
    }

    /**
     * Behavioral test demonstrating the use of the context with context-aware processors
     */
    @Test public void testMetadata() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testMetadata");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        final AbstractProcessor<String, Object> processor = new AbstractProcessor<String, Object>() {
            @Override public void process(final String key, final Object value) {
                context().forward("appId", context().applicationId());
                context().forward("taskId", context().taskId());

                context().forward("topic", context().topic());
                context().forward("partition", context().partition());
                context().forward("offset", context().offset());
                context().forward("timestamp", context().timestamp());

                context().forward("key", key);
                context().forward("value", value);
            }
        };

        final MockProcessorContext context = new MockProcessorContext(config);
        processor.init(context);

        try {
            processor.process("foo", 5L);
            Assert.fail("Should have thrown an NPE.");
        } catch (final NullPointerException npe) {
            // expected, since the record metadata isn't initialized
        }

        context.resetForwards();
        context.setRecordMetadata("t1", 0, 0L, 0L);

        {
            processor.process("foo", 5L);
            final Iterator<KeyValue> forwarded = context.forwarded().iterator();
            assertEquals(forwarded.next(), new KeyValue<>("appId", "testMetadata"));
            assertEquals(forwarded.next(), new KeyValue<>("taskId", new TaskId(0, 0)));
            assertEquals(forwarded.next(), new KeyValue<>("topic", "t1"));
            assertEquals(forwarded.next(), new KeyValue<>("partition", 0));
            assertEquals(forwarded.next(), new KeyValue<>("offset", 0L));
            assertEquals(forwarded.next(), new KeyValue<>("timestamp", 0L));
            assertEquals(forwarded.next(), new KeyValue<>("key", "foo"));
            assertEquals(forwarded.next(), new KeyValue<>("value", 5L));
        }

        context.resetForwards();

        // record metadata should be "sticky"
        context.setOffset(1L);
        context.setTimestamp(10L);

        {
            processor.process("bar", 50L);
            final Iterator<KeyValue> forwarded = context.forwarded().iterator();
            assertEquals(forwarded.next(), new KeyValue<>("appId", "testMetadata"));
            assertEquals(forwarded.next(), new KeyValue<>("taskId", new TaskId(0, 0)));
            assertEquals(forwarded.next(), new KeyValue<>("topic", "t1"));
            assertEquals(forwarded.next(), new KeyValue<>("partition", 0));
            assertEquals(forwarded.next(), new KeyValue<>("offset", 1L));
            assertEquals(forwarded.next(), new KeyValue<>("timestamp", 10L));
            assertEquals(forwarded.next(), new KeyValue<>("key", "bar"));
            assertEquals(forwarded.next(), new KeyValue<>("value", 50L));
        }

        context.resetForwards();
        // record metadata should be "sticky"
        context.setTopic("t2");
        context.setPartition(30);

        {
            processor.process("baz", 500L);
            final Iterator<KeyValue> forwarded = context.forwarded().iterator();
            assertEquals(forwarded.next(), new KeyValue<>("appId", "testMetadata"));
            assertEquals(forwarded.next(), new KeyValue<>("taskId", new TaskId(0, 0)));
            assertEquals(forwarded.next(), new KeyValue<>("topic", "t2"));
            assertEquals(forwarded.next(), new KeyValue<>("partition", 30));
            assertEquals(forwarded.next(), new KeyValue<>("offset", 1L));
            assertEquals(forwarded.next(), new KeyValue<>("timestamp", 10L));
            assertEquals(forwarded.next(), new KeyValue<>("key", "baz"));
            assertEquals(forwarded.next(), new KeyValue<>("value", 500L));
        }
    }

    /**
     * Behavioral test demonstrating testing captured punctuator behavior
     */
    @Test public void testPunctuatorCapture() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testPunctuatorCapture");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");


        final Processor<String, Long> processor = new Processor<String, Long>() {
            @Override public void init(final ProcessorContext context) {
                context.schedule(
                    1000L,
                    PunctuationType.WALL_CLOCK_TIME,
                    new Punctuator() {
                        @Override public void punctuate(final long timestamp) {
                            context.commit();
                        }
                    }
                );
            }

            @Override public void process(final String key, final Long value) {
            }

            @Override public void punctuate(final long timestamp) {
            }

            @Override public void close() {
            }
        };

        final MockProcessorContext context = new MockProcessorContext(config);

        processor.init(context);

        assertEquals(1000L, context.scheduledPunctuators().get(0).getIntervalMs());
        assertEquals(PunctuationType.WALL_CLOCK_TIME, context.scheduledPunctuators().get(0).getType());
        final Punctuator punctuator = context.scheduledPunctuators().get(0).getPunctuator();
        assertFalse(context.committed());
        punctuator.punctuate(1234L);
        assertTrue(context.committed());
    }

    /**
     * Unit test verifying the full MockProcessorContext constructor
     */
    @Test public void testFullConstructor() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testFullConstructor");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        final File dummyFile = new File("");
        final MockProcessorContext context = new MockProcessorContext(config, new TaskId(1, 1), dummyFile);

        assertEquals(context.applicationId(), "testFullConstructor");
        assertEquals(context.taskId(), new TaskId(1, 1));
        assertEquals(context.taskId(), new TaskId(1, 1));
        assertEquals("testFullConstructor", context.appConfigs().get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("testFullConstructor", context.appConfigsWithPrefix("application.").get("id"));
        assertEquals(Serdes.String().getClass(), context.keySerde().getClass());
        assertEquals(Serdes.Long().getClass(), context.valueSerde().getClass());
        assertEquals(dummyFile, context.stateDir());
    }
}
