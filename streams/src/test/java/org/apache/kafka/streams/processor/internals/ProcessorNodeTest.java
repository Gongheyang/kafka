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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ProcessorNodeTest {

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowStreamsExceptionIfExceptionCaughtDuringInit() {
        final ProcessorNode node = new ProcessorNode("name", new ExceptionalProcessor(), Collections.emptySet());
        assertThrows(StreamsException.class, () -> node.init(null));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowStreamsExceptionIfExceptionCaughtDuringClose() {
        final ProcessorNode node = new ProcessorNode("name", new ExceptionalProcessor(), Collections.emptySet());
        assertThrows(StreamsException.class, () -> node.init(null));
    }

    @SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
    private static class ExceptionalProcessor implements org.apache.kafka.streams.processor.Processor<Object, Object> {
        @Override
        public void init(final ProcessorContext context) {
            throw new RuntimeException();
        }

        @Override
        public void process(final Object key, final Object value) {
            throw new RuntimeException();
        }

        @Override
        public void close() {
            throw new RuntimeException();
        }
    }

    @SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
    private static class NoOpProcessor implements org.apache.kafka.streams.processor.Processor<Object, Object> {
        @Override
        public void init(final ProcessorContext context) {

        }

        @Override
        public void process(final Object key, final Object value) {

        }

        @Override
        public void close() {

        }
    }

    @Test
    public void testMetricsWithBuiltInMetricsVersionLatest() {
        final Metrics metrics = new Metrics();
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test-client", StreamsConfig.METRICS_LATEST, new MockTime());
        final InternalMockProcessorContext<Object, Object> context = new InternalMockProcessorContext<>(streamsMetrics);
        final ProcessorNode<Object, Object, Object, Object> node = new ProcessorNode<>("name", new NoOpProcessor(), Collections.<String>emptySet());
        node.init(context);

        final String threadId = Thread.currentThread().getName();
        final String[] latencyOperations = {"process", "punctuate", "create", "destroy"};
        final String groupName = "stream-processor-node-metrics";
        final Map<String, String> metricTags = new LinkedHashMap<>();
        final String threadIdTagKey = "client-id";
        metricTags.put("processor-node-id", node.name());
        metricTags.put("task-id", context.taskId().toString());
        metricTags.put(threadIdTagKey, threadId);

        for (final String opName : latencyOperations) {
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-avg", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-max", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-rate", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-total", groupName, metricTags));
        }

        // test parent sensors
        metricTags.put("processor-node-id", ROLLUP_VALUE);
        for (final String opName : latencyOperations) {
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-avg", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-max", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-rate", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-total", groupName, metricTags));
        }
    }

    @Test
    public void testTopologyLevelConfigException() {
        // Serdes configuration is missing and no default is set which will trigger an exception
        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream("streams-plaintext-input")
            .flatMapValues(value -> {
                return Collections.singletonList("");
            });
        final Topology topology = builder.build();

        final TopologyTestDriver testDriver = new TopologyTestDriver(topology);
        final TestInputTopic<String, String> topic = testDriver.createInputTopic("streams-plaintext-input", new StringSerializer(), new StringSerializer());

        final ConfigException se = assertThrows(ConfigException.class, () -> topic.pipeInput("a-key", "a value"));
        final String msg = se.getMessage();
        assertTrue("Error about class cast with serdes", msg.contains("default.key.serde"));
        assertTrue("Error about class cast with serdes", msg.contains("specify a key serde"));
    }

    private static class ClassCastProcessor extends ExceptionalProcessor {

        @Override
        public void init(final ProcessorContext context) {
        }

        @Override
        public void process(final Object key, final Object value) {
            throw new ClassCastException("Incompatible types simulation exception.");
        }
    }

    @Test
    public void testTopologyLevelClassCastExceptionDirect() {
        final Metrics metrics = new Metrics();
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test-client", StreamsConfig.METRICS_LATEST, new MockTime());
        final InternalMockProcessorContext<Object, Object> context = new InternalMockProcessorContext<>(streamsMetrics);
        final ProcessorNode<Object, Object, Object, Object> node = new ProcessorNode<>("name", new ClassCastProcessor(), Collections.emptySet());
        node.init(context);
        final StreamsException se = assertThrows(
            StreamsException.class,
            () -> node.process(new Record<>("aKey", "aValue", 0))
        );
        assertThat(se.getCause(), instanceOf(ClassCastException.class));
        assertThat(se.getMessage(), containsString("default Serdes"));
        assertThat(se.getMessage(), containsString("input types"));
    }
}
