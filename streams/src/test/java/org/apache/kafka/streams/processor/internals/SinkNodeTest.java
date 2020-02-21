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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class SinkNodeTest {
    private final Serializer<byte[]> anySerializer = Serdes.ByteArray().serializer();
    private final RecordCollector recordCollector = new MockRecordCollector() {
        @Override
        public <K, V> void send(final String topic,
                                final K key,
                                final V value,
                                final Headers headers,
                                final Long timestamp,
                                final Serializer<K> keySerializer,
                                final Serializer<V> valueSerializer,
                                final StreamPartitioner<? super K, ? super V> partitioner) {
            throw new ClassCastException("boom");
        }
    };

    private final MockInternalProcessorContext context;
    private final SinkNode<byte[], byte[]> sink = new SinkNode<>("anyNodeName",
            new StaticTopicNameExtractor<>("any-output-topic"), anySerializer, anySerializer, null);

    // Used to verify that the correct exceptions are thrown if the compiler checks are bypassed
    @SuppressWarnings("unchecked")
    private final SinkNode<Object, Object> illTypedSink = (SinkNode) sink;

    public SinkNodeTest() {
        final Properties properties = StreamsTestUtils.getStreamsConfig();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.BytesSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.BytesSerde.class);
        context = new MockInternalProcessorContext(properties);
        context.setRecordCollector(recordCollector);
    }

    @Before
    public void before() {
        sink.init(context);
    }

    @Test
    public void shouldThrowStreamsExceptionOnInputRecordWithInvalidTimestamp() {
        // When/Then
        context.setTimestamp(-1); // ensures a negative timestamp is set for the record we send next
        try {
            illTypedSink.process("any key".getBytes(), "any value".getBytes());
            fail("Should have thrown StreamsException");
        } catch (final StreamsException ignored) {
            // expected
        }
    }

    @Test
    public void shouldThrowStreamsExceptionWithClassCastFromRecordCollector() {
        // When/Then
        context.setTimestamp(0);
        try {
            illTypedSink.process("key", "value");
            fail("Should have thrown StreamsException");
        } catch (final StreamsException e) {
            assertThat(e.getCause(), instanceOf(ClassCastException.class));
        }
    }

    @Test
    public void shouldThrowStreamsExceptionNullKeyWithClassCastFromRecordCollector() {
        // When/Then
        context.setTimestamp(1);
        try {
            illTypedSink.process(null, "");
            fail("Should have thrown StreamsException");
        } catch (final StreamsException e) {
            assertThat(e.getCause(), instanceOf(ClassCastException.class));
            assertThat(e.getMessage(), containsString("unknown because key is null"));
        }
    }

    @Test
    public void shouldThrowStreamsExceptionNullValueWithClassCastFromRecordCollector() {
        // When/Then
        context.setTimestamp(1);
        try {
            illTypedSink.process("", null);
            fail("Should have thrown StreamsException");
        } catch (final StreamsException e) {
            assertThat(e.getCause(), instanceOf(ClassCastException.class));
            assertThat(e.getMessage(), containsString("unknown because value is null"));
        }
    }

}
