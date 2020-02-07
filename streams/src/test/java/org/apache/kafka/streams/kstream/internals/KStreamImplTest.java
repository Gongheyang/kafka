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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class KStreamImplTest {

    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());
    @SuppressWarnings("deprecation")
    private final org.apache.kafka.streams.kstream.Serialized<String, String> stringSerialized =
        org.apache.kafka.streams.kstream.Serialized.with(Serdes.String(), Serdes.String());

    private final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
    private final TransformerSupplier<String, String, KeyValue<String, String>> transformerSupplier =
        () -> new Transformer<String, String, KeyValue<String, String>>() {
            @Override
            public void init(final ProcessorContext context) {}

            @Override
            public KeyValue<String, String> transform(final String key, final String value) {
                return new KeyValue<>(key, value);
            }

            @Override
            public void close() {}
        };
    private final TransformerSupplier<String, String, Iterable<KeyValue<String, String>>> flatTransformerSupplier =
        () -> new Transformer<String, String, Iterable<KeyValue<String, String>>>() {
            @Override
            public void init(final ProcessorContext context) {}

            @Override
            public Iterable<KeyValue<String, String>> transform(final String key, final String value) {
                return Collections.singleton(new KeyValue<>(key, value));
            }

            @Override
            public void close() {}
        };
    private final ValueTransformerSupplier<String, String> valueTransformerSupplier =
        () -> new ValueTransformer<String, String>() {
            @Override
            public void init(final ProcessorContext context) {}

            @Override
            public String transform(final String value) {
                return value;
            }

            @Override
            public void close() {}
        };
    private final ValueTransformerWithKeySupplier<String, String, String> valueTransformerWithKeySupplier =
        () -> new ValueTransformerWithKey<String, String, String>() {
            @Override
            public void init(final ProcessorContext context) {}

            @Override
            public String transform(final String key, final String value) {
                return value;
            }

            @Override
            public void close() {}
        };
    private final ValueTransformerSupplier<String, Iterable<String>> flatValueTransformerSupplier =
        () -> new ValueTransformer<String, Iterable<String>>() {
            @Override
            public void init(final ProcessorContext context) {}

            @Override
            public Iterable<String> transform(final String value) {
                return Collections.singleton(value);
            }

            @Override
            public void close() {}
        };
    private final ValueTransformerWithKeySupplier<String, String, Iterable<String>> flatValueTransformerWithKeySupplier =
        () -> new ValueTransformerWithKey<String, String, Iterable<String>>() {
            @Override
            public void init(final ProcessorContext context) {}

            @Override
            public Iterable<String> transform(final String key, final String value) {
                return Collections.singleton(value);
            }

            @Override
            public void close() {}
        };

    private StreamsBuilder builder;
    private KStream<String, String> testStream;
    private KTable<String, String> testTable;
    private GlobalKTable<String, String> testGlobalTable;

    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    private final Serde<String> mySerde = new Serdes.StringSerde();

    @Before
    public void before() {
        builder = new StreamsBuilder();
        testStream = builder.stream("source");
        testTable = builder.table("topic");
        testGlobalTable = builder.globalTable("global");
    }

    @Test
    public void shouldNotAllowNullPredicateOnFilter() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.filter(null));
        assertThat(exception.getMessage(), equalTo("predicate can't be null"));
    }

    @Test
    public void shouldNotAllowNullPredicateOnFilterWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.filter(null, Named.as("filter")));
        assertThat(exception.getMessage(), equalTo("predicate can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnFilter() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.filter((k, v) -> true, null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullPredicateOnFilterNot() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.filterNot(null));
        assertThat(exception.getMessage(), equalTo("predicate can't be null"));
    }

    @Test
    public void shouldNotAllowNullPredicateOnFilterNotWithName() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.filterNot(null, Named.as("filter")));
        assertThat(exception.getMessage(), equalTo("predicate can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnFilterNot() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.filterNot((k, v) -> true, null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnSelectKey() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.selectKey(null));
        assertThat(exception.getMessage(), equalTo("mapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnSelectKeyWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.selectKey(null, Named.as("keySelector")));
        assertThat(exception.getMessage(), equalTo("mapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnSelectKey() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.selectKey((k, v) -> k, null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnMap() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.map(null));
        assertThat(exception.getMessage(), equalTo("mapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnMapWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.map(null, Named.as("map")));
        assertThat(exception.getMessage(), equalTo("mapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnMap() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.map(KeyValue::pair, null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnMapValues() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.mapValues((ValueMapper<Object, Object>) null));
        assertThat(exception.getMessage(), equalTo("valueMapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnMapValuesWithKey() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.mapValues((ValueMapperWithKey<Object, Object, Object>) null));
        assertThat(exception.getMessage(), equalTo("valueMapperWithKey can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnMapValuesWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.mapValues((ValueMapper<Object, Object>) null, Named.as("valueMapper")));
        assertThat(exception.getMessage(), equalTo("valueMapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnMapValuesWithKeyWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.mapValues(
                (ValueMapperWithKey<Object, Object, Object>) null,
                Named.as("valueMapperWithKey")));
        assertThat(exception.getMessage(), equalTo("valueMapperWithKey can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnMapValues() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.mapValues(v -> v, null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnMapValuesWithKey() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.mapValues((k, v) -> v, null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnFlatMap() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatMap(null));
        assertThat(exception.getMessage(), equalTo("mapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnFlatMapWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatMap(null, Named.as("flatMapper")));
        assertThat(exception.getMessage(), equalTo("mapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnFlatMap() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatMap((k, v) -> Collections.emptyList(), null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnFlatMapValues() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatMapValues((ValueMapper<Object, Iterable<Object>>) null));
        assertThat(exception.getMessage(), equalTo("valueMapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnFlatMapValuesWithKey() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatMapValues((ValueMapperWithKey<Object, Object, ? extends Iterable<Object>>) null));
        assertThat(exception.getMessage(), equalTo("valueMapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnFlatMapValuesWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatMapValues(
                (ValueMapper<Object, Iterable<Object>>) null,
                Named.as("flatValueMapper")));
        assertThat(exception.getMessage(), equalTo("valueMapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnFlatMapValuesWithKeyWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatMapValues(
                (ValueMapperWithKey<Object, Object, ? extends Iterable<Object>>) null,
                Named.as("flatValueMapperWithKey")));
        assertThat(exception.getMessage(), equalTo("valueMapper can't be null"));
    }

    @Test
    public void shouldNotAllowNullNameOnFlatMapValues() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatMapValues(v -> Collections.emptyList(), null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullNameOnFlatMapValuesWithKey() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatMapValues((k, v) -> Collections.emptyList(), null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullPrintedOnPrint() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.print(null));
        assertThat(exception.getMessage(), equalTo("printed can't be null"));
    }

    @Test
    public void shouldNotAllowNullActionOnForEach() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.foreach(null));
        assertThat(exception.getMessage(), equalTo("action can't be null"));
    }

    @Test
    public void shouldNotAllowNullActionOnForEachWithName() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.foreach(null, Named.as("foreach")));
        assertThat(exception.getMessage(), equalTo("action can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnForEach() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.foreach((k, v) -> { }, null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullActionOnPeek() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.peek(null));
        assertThat(exception.getMessage(), equalTo("action can't be null"));
    }

    @Test
    public void shouldNotAllowNullActionOnPeekWithName() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.peek(null, Named.as("peek")));
        assertThat(exception.getMessage(), equalTo("action can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnPeek() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.peek((k, v) -> { }, null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void shouldNotAllowNullPredicatedOnBranch() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.branch((Predicate[]) null));
        assertThat(exception.getMessage(), equalTo("predicates can't be a null array"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldHaveAtLeastOnPredicateWhenBranching() {
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> testStream.branch());
        assertThat(exception.getMessage(), equalTo("branch() requires at least one predicate"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldHaveAtLeastOnPredicateWhenBranchingWithNamed() {
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> testStream.branch(Named.as("branch")));
        assertThat(exception.getMessage(), equalTo("branch() requires at least one predicate"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotAllowNullNamedOnBranch() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.branch((Named) null, (k, v) -> true));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCantHaveNullPredicate() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.branch((Predicate<Object, Object>) null));
        assertThat(exception.getMessage(), equalTo("predicates can't be null"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCantHaveNullPredicateWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.branch(Named.as("branch"), (Predicate<Object, Object>) null));
        assertThat(exception.getMessage(), equalTo("predicates can't be null"));
    }

    @Test
    public void shouldNotAllowNullKStreamOnMerge() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.merge(null));
        assertThat(exception.getMessage(), equalTo("stream can't be null"));
    }

    @Test
    public void shouldNotAllowNullKStreamOnMergeWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.merge(null, Named.as("merge")));
        assertThat(exception.getMessage(), equalTo("stream can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnMerge() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.merge(testStream, null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullTopicOnThrough() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.through(null));
        assertThat(exception.getMessage(), equalTo("topic can't be null"));
    }

    @Test
    public void shouldNotAllowNullTopicOnThroughWithProduced() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.through(null, Produced.as("through")));
        assertThat(exception.getMessage(), equalTo("topic can't be null"));
    }

    @Test
    public void shouldNotAllowNullProducedOnThrough() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.through("topic", null));
        assertThat(exception.getMessage(), equalTo("produced can't be null"));
    }

    @Test
    public void shouldNotAllowNullTopicOnTo() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.to((String) null));
        assertThat(exception.getMessage(), equalTo("topic can't be null"));
    }

    @Test
    public void shouldNotAllowNullTopicChooserOnTo() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.to((TopicNameExtractor<String, String>) null));
        assertThat(exception.getMessage(), equalTo("topicExtractor can't be null"));
    }

    @Test
    public void shouldNotAllowNullTopicOnToWithProduced() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.to((String) null, Produced.as("to")));
        assertThat(exception.getMessage(), equalTo("topic can't be null"));
    }

    @Test
    public void shouldNotAllowNullTopicChooserOnToWithProduced() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.to((TopicNameExtractor<String, String>) null, Produced.as("to")));
        assertThat(exception.getMessage(), equalTo("topicExtractor can't be null"));
    }

    @Test
    public void shouldNotAllowNullProducedOnToWithTopicName() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.to("topic", null));
        assertThat(exception.getMessage(), equalTo("produced can't be null"));
    }

    @Test
    public void shouldNotAllowNullProducedOnToWithTopicChooser() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.to((k, v, ctx) -> "topic", null));
        assertThat(exception.getMessage(), equalTo("produced can't be null"));
    }

    @Test
    public void shouldNotAllowNullSelectorOnGroupBy() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.groupBy(null));
        assertThat(exception.getMessage(), equalTo("keySelector can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullSelectorOnGroupByWithSerialized() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.groupBy(null, stringSerialized));
        assertThat(exception.getMessage(), equalTo("keySelector can't be null"));
    }

    @Test
    public void shouldNotAllowNullSelectorOnGroupByWithGrouped() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.groupBy(null, Grouped.as("name")));
        assertThat(exception.getMessage(), equalTo("keySelector can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullSerializedOnGroupBy() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.groupBy((k, v) -> k, (org.apache.kafka.streams.kstream.Serialized<String, String>) null));
        assertThat(exception.getMessage(), equalTo("serialized can't be null"));
    }

    @Test
    public void shouldNotAllowNullGroupedOnGroupBy() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.groupBy((k, v) -> k, (Grouped<String, String>) null));
        assertThat(exception.getMessage(), equalTo("grouped can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullSerializedOnGroupByKey() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.groupByKey((org.apache.kafka.streams.kstream.Serialized<String, String>) null));
        assertThat(exception.getMessage(), equalTo("serialized can't be null"));
    }

    @Test
    public void shouldNotAllowNullGroupedOnGroupByKey() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.groupByKey((Grouped<String, String>) null));
        assertThat(exception.getMessage(), equalTo("grouped can't be null"));
    }

    @Test
    public void shouldNotAllowNullOtherStreamOnJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(null, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(ofMillis(10))));
        assertThat(exception.getMessage(), equalTo("otherStream can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullOtherStreamOnJoinWithJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(
                null,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("otherStream can't be null"));
    }

    @Test
    public void shouldNotAllowNullOtherStreamOnJoinWithStreamJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(
                null,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                StreamJoined.as("name")));
        assertThat(exception.getMessage(), equalTo("otherStream can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueJoinerOnJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(testStream, null, JoinWindows.of(ofMillis(10))));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullValueJoinerOnJoinWithJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(
                testStream,
                null,
                JoinWindows.of(ofMillis(10)),
                Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueJoinerOnJoinWithStreamJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(
                testStream,
                null,
                JoinWindows.of(ofMillis(10)),
                StreamJoined.as("name")));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinWindowsOnJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(testStream, MockValueJoiner.TOSTRING_JOINER, null));
        assertThat(exception.getMessage(), equalTo("windows can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullJoinWindowsOnJoinWithJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                null,
                Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("windows can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinWindowsOnJoinWithStreamJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                null,
                StreamJoined.as("name")));
        assertThat(exception.getMessage(), equalTo("windows can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullJoinedOnJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                (Joined<String, String, String>) null));
        assertThat(exception.getMessage(), equalTo("joined can't be null"));
    }

    @Test
    public void shouldNotAllowNullStreamJoinedOnJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                (StreamJoined<String, String, String>) null));
        assertThat(exception.getMessage(), equalTo("streamJoined can't be null"));
    }

    @Test
    public void shouldNotAllowNullOtherStreamOnLeftJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(null, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(ofMillis(10))));
        assertThat(exception.getMessage(), equalTo("otherStream can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullOtherStreamOnLeftJoinWithJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(
                null,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("otherStream can't be null"));
    }

    @Test
    public void shouldNotAllowNullOtherStreamOnLeftJoinWithStreamJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(
                null,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                StreamJoined.as("name")));
        assertThat(exception.getMessage(), equalTo("otherStream can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueJoinerOnLeftJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(testStream, null, JoinWindows.of(ofMillis(10))));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullValueJoinerOnLeftJoinWithJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(
                testStream,
                null,
                JoinWindows.of(ofMillis(10)),
                Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueJoinerOnLeftJoinWithStreamJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(
                testStream,
                null,
                JoinWindows.of(ofMillis(10)),
                StreamJoined.as("name")));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinWindowsOnLeftJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(testStream, MockValueJoiner.TOSTRING_JOINER, null));
        assertThat(exception.getMessage(), equalTo("windows can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullJoinWindowsOnLeftJoinWithJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                null,
                Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("windows can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinWindowsOnLeftJoinWithStreamJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                null,
                StreamJoined.as("name")));
        assertThat(exception.getMessage(), equalTo("windows can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullJoinedOnLeftJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                (Joined<String, String, String>) null));
        assertThat(exception.getMessage(), equalTo("joined can't be null"));
    }

    @Test
    public void shouldNotAllowNullStreamJoinedOnLeftJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                (StreamJoined<String, String, String>) null));
        assertThat(exception.getMessage(), equalTo("streamJoined can't be null"));
    }

    @Test
    public void shouldNotAllowNullOtherStreamOnOuterJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.outerJoin(null, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(ofMillis(10))));
        assertThat(exception.getMessage(), equalTo("otherStream can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullOtherStreamOnOuterJoinWithJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.outerJoin(
                null,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("otherStream can't be null"));
    }

    @Test
    public void shouldNotAllowNullOtherStreamOnOuterJoinWithStreamJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.outerJoin(
                null,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                StreamJoined.as("name")));
        assertThat(exception.getMessage(), equalTo("otherStream can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueJoinerOnOuterJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.outerJoin(testStream, null, JoinWindows.of(ofMillis(10))));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullValueJoinerOnOuterJoinWithJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.outerJoin(
                testStream,
                null,
                JoinWindows.of(ofMillis(10)),
                Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueJoinerOnOuterJoinWithStreamJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.outerJoin(
                testStream,
                null,
                JoinWindows.of(ofMillis(10)),
                StreamJoined.as("name")));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinWindowsOnOuterJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.outerJoin(testStream, MockValueJoiner.TOSTRING_JOINER, null));
        assertThat(exception.getMessage(), equalTo("windows can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullJoinWindowsOnOuterJoinWithJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.outerJoin(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                null,
                Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("windows can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinWindowsOnOuterJoinWithStreamJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.outerJoin(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                null,
                StreamJoined.as("name")));
        assertThat(exception.getMessage(), equalTo("windows can't be null"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowNullJoinedOnOuterJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.outerJoin(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                (Joined<String, String, String>) null));
        assertThat(exception.getMessage(), equalTo("joined can't be null"));
    }

    @Test
    public void shouldNotAllowNullStreamJoinedOnOuterJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.outerJoin(
                testStream,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(10)),
                (StreamJoined<String, String, String>) null));
        assertThat(exception.getMessage(), equalTo("streamJoined can't be null"));
    }

    @Test
    public void shouldNotAllowNullTableOnTableJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(null, MockValueJoiner.TOSTRING_JOINER));
        assertThat(exception.getMessage(), equalTo("table can't be null"));
    }

    @Test
    public void shouldNotAllowNullTableOnTableJoinWithJoiner() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(null, MockValueJoiner.TOSTRING_JOINER, Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("table can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueMapperOnTableJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(testTable, null));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueMapperOnTableJoinWithJoiner() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(testTable, null, Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinedOnTableJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(testTable, MockValueJoiner.TOSTRING_JOINER, null));
        assertThat(exception.getMessage(), equalTo("joined can't be null"));
    }

    @Test
    public void shouldNotAllowNullTableOnTableLeftJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(null, MockValueJoiner.TOSTRING_JOINER));
        assertThat(exception.getMessage(), equalTo("table can't be null"));
    }

    @Test
    public void shouldNotAllowNullTableOnTableLeftJoinWithJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(null, MockValueJoiner.TOSTRING_JOINER, Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("table can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueMapperOnTableLeftJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(testTable, null));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueMapperOnTableLeftJoinWithJoined() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(testTable, null, Joined.as("name")));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinedOnTableLeftJoin() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(testTable, MockValueJoiner.TOSTRING_JOINER, null));
        assertThat(exception.getMessage(), equalTo("joined can't be null"));
    }

    @Test
    public void shouldNotAllowNullTableOnJoinWithGlobalTable() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(null, MockMapper.selectValueMapper(), MockValueJoiner.TOSTRING_JOINER));
        assertThat(exception.getMessage(), equalTo("globalTable can't be null"));
    }

    @Test
    public void shouldNotAllowNullTableOnJoinWithGlobalTableWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(
                null,
                MockMapper.selectValueMapper(),
                MockValueJoiner.TOSTRING_JOINER,
                Named.as("name")));
        assertThat(exception.getMessage(), equalTo("globalTable can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnJoinWithGlobalTable() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(testGlobalTable, null, MockValueJoiner.TOSTRING_JOINER));
        assertThat(exception.getMessage(), equalTo("keySelector can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnJoinWithGlobalTableWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(
                testGlobalTable,
                null,
                MockValueJoiner.TOSTRING_JOINER,
                Named.as("name")));
        assertThat(exception.getMessage(), equalTo("keySelector can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinerOnJoinWithGlobalTable() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(testGlobalTable, MockMapper.selectValueMapper(), null));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinerOnJoinWithGlobalTableWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.join(
                testGlobalTable,
                MockMapper.selectValueMapper(),
                null,
                Named.as("name")));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullTableOnLeftJoinWithGlobalTable() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(null, MockMapper.selectValueMapper(), MockValueJoiner.TOSTRING_JOINER));
        assertThat(exception.getMessage(), equalTo("globalTable can't be null"));
    }

    @Test
    public void shouldNotAllowNullTableOnLeftJoinWithGlobalTableWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(
                null,
                MockMapper.selectValueMapper(),
                MockValueJoiner.TOSTRING_JOINER,
                Named.as("name")));
        assertThat(exception.getMessage(), equalTo("globalTable can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnLeftJoinWithGlobalTable() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(testGlobalTable, null, MockValueJoiner.TOSTRING_JOINER));
        assertThat(exception.getMessage(), equalTo("keySelector can't be null"));
    }

    @Test
    public void shouldNotAllowNullMapperOnLeftJoinWithGlobalTableWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(
                testGlobalTable,
                null,
                MockValueJoiner.TOSTRING_JOINER,
                Named.as("name")));
        assertThat(exception.getMessage(), equalTo("keySelector can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinerOnLeftJoinWithGlobalTable() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(testGlobalTable, MockMapper.selectValueMapper(), null));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @Test
    public void shouldNotAllowNullJoinerOnLeftJoinWithGlobalTableWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.leftJoin(
                testGlobalTable,
                MockMapper.selectValueMapper(),
                null,
                Named.as("name")));
        assertThat(exception.getMessage(), equalTo("joiner can't be null"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNumProcesses() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> source1 = builder.stream(Arrays.asList("topic-1", "topic-2"), stringConsumed);

        final KStream<String, String> source2 = builder.stream(Arrays.asList("topic-3", "topic-4"), stringConsumed);

        final KStream<String, String> stream1 = source1.filter((key, value) -> true)
            .filterNot((key, value) -> false);

        final KStream<String, Integer> stream2 = stream1.mapValues(Integer::new);

        final KStream<String, Integer> stream3 = source2.flatMapValues((ValueMapper<String, Iterable<Integer>>)
            value -> Collections.singletonList(Integer.valueOf(value)));

        final KStream<String, Integer>[] streams2 = stream2.branch(
            (key, value) -> (value % 2) == 0,
            (key, value) -> true
        );

        final KStream<String, Integer>[] streams3 = stream3.branch(
            (key, value) -> (value % 2) == 0,
            (key, value) -> true
        );

        final int anyWindowSize = 1;
        final StreamJoined<String, Integer, Integer> joined = StreamJoined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer());
        final KStream<String, Integer> stream4 = streams2[0].join(streams3[0],
            Integer::sum, JoinWindows.of(ofMillis(anyWindowSize)), joined);

        streams2[1].join(streams3[1], Integer::sum,
            JoinWindows.of(ofMillis(anyWindowSize)), joined);

        stream4.to("topic-5");

        streams2[1].through("topic-6").process(new MockProcessorSupplier<>());

        assertEquals(2 + // sources
                2 + // stream1
                1 + // stream2
                1 + // stream3
                1 + 2 + // streams2
                1 + 2 + // streams3
                5 * 2 + // stream2-stream3 joins
                1 + // to
                2 + // through
                1, // process
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("X").build().processors().size());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void shouldPreserveSerdesForOperators() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream(Collections.singleton("topic-1"), stringConsumed);
        final KTable<String, String> table1 = builder.table("topic-2", stringConsumed);
        final GlobalKTable<String, String> table2 = builder.globalTable("topic-2", stringConsumed);
        final ConsumedInternal<String, String> consumedInternal = new ConsumedInternal<>(stringConsumed);

        final KeyValueMapper<String, String, String> selector = (key, value) -> key;
        final KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> flatSelector = (key, value) -> Collections.singleton(new KeyValue<>(key, value));
        final ValueMapper<String, String> mapper = value -> value;
        final ValueMapper<String, Iterable<String>> flatMapper = Collections::singleton;
        final ValueJoiner<String, String, String> joiner = (value1, value2) -> value1;

        assertEquals(((AbstractStream) stream1.filter((key, value) -> false)).keySerde(), consumedInternal.keySerde());
        assertEquals(((AbstractStream) stream1.filter((key, value) -> false)).valueSerde(), consumedInternal.valueSerde());

        assertEquals(((AbstractStream) stream1.filterNot((key, value) -> false)).keySerde(), consumedInternal.keySerde());
        assertEquals(((AbstractStream) stream1.filterNot((key, value) -> false)).valueSerde(), consumedInternal.valueSerde());

        assertNull(((AbstractStream) stream1.selectKey(selector)).keySerde());
        assertEquals(((AbstractStream) stream1.selectKey(selector)).valueSerde(), consumedInternal.valueSerde());

        assertNull(((AbstractStream) stream1.map(KeyValue::new)).keySerde());
        assertNull(((AbstractStream) stream1.map(KeyValue::new)).valueSerde());

        assertEquals(((AbstractStream) stream1.mapValues(mapper)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.mapValues(mapper)).valueSerde());

        assertNull(((AbstractStream) stream1.flatMap(flatSelector)).keySerde());
        assertNull(((AbstractStream) stream1.flatMap(flatSelector)).valueSerde());

        assertEquals(((AbstractStream) stream1.flatMapValues(flatMapper)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.flatMapValues(flatMapper)).valueSerde());

        assertNull(((AbstractStream) stream1.transform(transformerSupplier)).keySerde());
        assertNull(((AbstractStream) stream1.transform(transformerSupplier)).valueSerde());

        assertEquals(((AbstractStream) stream1.transformValues(valueTransformerSupplier)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.transformValues(valueTransformerSupplier)).valueSerde());

        assertNull(((AbstractStream) stream1.merge(stream1)).keySerde());
        assertNull(((AbstractStream) stream1.merge(stream1)).valueSerde());

        assertEquals(((AbstractStream) stream1.through("topic-3")).keySerde(), consumedInternal.keySerde());
        assertEquals(((AbstractStream) stream1.through("topic-3")).valueSerde(), consumedInternal.valueSerde());
        assertEquals(((AbstractStream) stream1.through("topic-3", Produced.with(mySerde, mySerde))).keySerde(), mySerde);
        assertEquals(((AbstractStream) stream1.through("topic-3", Produced.with(mySerde, mySerde))).valueSerde(), mySerde);

        assertEquals(((AbstractStream) stream1.groupByKey()).keySerde(), consumedInternal.keySerde());
        assertEquals(((AbstractStream) stream1.groupByKey()).valueSerde(), consumedInternal.valueSerde());
        assertEquals(((AbstractStream) stream1.groupByKey(Grouped.with(mySerde, mySerde))).keySerde(), mySerde);
        assertEquals(((AbstractStream) stream1.groupByKey(Grouped.with(mySerde, mySerde))).valueSerde(), mySerde);

        assertNull(((AbstractStream) stream1.groupBy(selector)).keySerde());
        assertEquals(((AbstractStream) stream1.groupBy(selector)).valueSerde(), consumedInternal.valueSerde());
        assertEquals(((AbstractStream) stream1.groupBy(selector, Grouped.with(mySerde, mySerde))).keySerde(), mySerde);
        assertEquals(((AbstractStream) stream1.groupBy(selector, Grouped.with(mySerde, mySerde))).valueSerde(), mySerde);

        assertNull(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).keySerde());
        assertNull(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).valueSerde());
        assertEquals(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), StreamJoined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
        assertNull(((AbstractStream) stream1.join(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), StreamJoined.with(mySerde, mySerde, mySerde))).valueSerde());

        assertNull(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).keySerde());
        assertNull(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).valueSerde());
        assertEquals(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), StreamJoined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
        assertNull(((AbstractStream) stream1.leftJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), StreamJoined.with(mySerde, mySerde, mySerde))).valueSerde());

        assertNull(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).keySerde());
        assertNull(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)))).valueSerde());
        assertEquals(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), StreamJoined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
        assertNull(((AbstractStream) stream1.outerJoin(stream1, joiner, JoinWindows.of(Duration.ofMillis(100L)), StreamJoined.with(mySerde, mySerde, mySerde))).valueSerde());

        assertEquals(((AbstractStream) stream1.join(table1, joiner)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.join(table1, joiner)).valueSerde());
        assertEquals(((AbstractStream) stream1.join(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
        assertNull(((AbstractStream) stream1.join(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).valueSerde());

        assertEquals(((AbstractStream) stream1.leftJoin(table1, joiner)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.leftJoin(table1, joiner)).valueSerde());
        assertEquals(((AbstractStream) stream1.leftJoin(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).keySerde(), mySerde);
        assertNull(((AbstractStream) stream1.leftJoin(table1, joiner, Joined.with(mySerde, mySerde, mySerde))).valueSerde());

        assertEquals(((AbstractStream) stream1.join(table2, selector, joiner)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.join(table2, selector, joiner)).valueSerde());

        assertEquals(((AbstractStream) stream1.leftJoin(table2, selector, joiner)).keySerde(), consumedInternal.keySerde());
        assertNull(((AbstractStream) stream1.leftJoin(table2, selector, joiner)).valueSerde());
    }

    @Test
    public void shouldUseRecordMetadataTimestampExtractorWithThrough() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream1 = builder.stream(Arrays.asList("topic-1", "topic-2"), stringConsumed);
        final KStream<String, String> stream2 = builder.stream(Arrays.asList("topic-3", "topic-4"), stringConsumed);

        stream1.to("topic-5");
        stream2.through("topic-6");

        final ProcessorTopology processorTopology = TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("X").build();
        assertThat(processorTopology.source("topic-6").getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp.class));
        assertNull(processorTopology.source("topic-4").getTimestampExtractor());
        assertNull(processorTopology.source("topic-3").getTimestampExtractor());
        assertNull(processorTopology.source("topic-2").getTimestampExtractor());
        assertNull(processorTopology.source("topic-1").getTimestampExtractor());
    }

    @Test
    public void shouldSendDataThroughTopicUsingProduced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "topic";
        final KStream<String, String> stream = builder.stream(input, stringConsumed);
        stream.through("through-topic", Produced.with(Serdes.String(), Serdes.String())).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic.pipeInput("a", "b");
        }
        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Collections.singletonList(new KeyValueTimestamp<>("a", "b", 0))));
    }

    @Test
    public void shouldSendDataToTopicUsingProduced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "topic";
        final KStream<String, String> stream = builder.stream(input, stringConsumed);
        stream.to("to-topic", Produced.with(Serdes.String(), Serdes.String()));
        builder.stream("to-topic", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic.pipeInput("e", "f");
        }
        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Collections.singletonList(new KeyValueTimestamp<>("e", "f", 0))));
    }

    @Test
    public void shouldSendDataToDynamicTopics() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "topic";
        final KStream<String, String> stream = builder.stream(input, stringConsumed);
        stream.to((key, value, context) -> context.topic() + "-" + key + "-" + value.substring(0, 1),
            Produced.with(Serdes.String(), Serdes.String()));
        builder.stream(input + "-a-v", stringConsumed).process(processorSupplier);
        builder.stream(input + "-b-v", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic.pipeInput("a", "v1");
            inputTopic.pipeInput("a", "v2");
            inputTopic.pipeInput("b", "v1");
        }
        final List<MockProcessor<String, String>> mockProcessors = processorSupplier.capturedProcessors(2);
        assertThat(mockProcessors.get(0).processed, equalTo(asList(new KeyValueTimestamp<>("a", "v1", 0),
            new KeyValueTimestamp<>("a", "v2", 0))));
        assertThat(mockProcessors.get(1).processed, equalTo(Collections.singletonList(new KeyValueTimestamp<>("b", "v1", 0))));
    }

    @SuppressWarnings({"rawtypes", "deprecation"}) // specifically testing the deprecated variant
    @Test
    public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreatedWithRetention() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> kStream = builder.stream("topic-1", stringConsumed);
        final ValueJoiner<String, String, String> valueJoiner = MockValueJoiner.instance(":");
        final long windowSize = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
        final KStream<String, String> stream = kStream
            .map((key, value) -> KeyValue.pair(value, value));
        stream.join(kStream,
            valueJoiner,
            JoinWindows.of(ofMillis(windowSize)).grace(ofMillis(3 * windowSize)),
            Joined.with(Serdes.String(),
                Serdes.String(),
                Serdes.String()))
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        final ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("X").build();

        final SourceNode originalSourceNode = topology.source("topic-1");

        for (final SourceNode sourceNode : topology.sources()) {
            if (sourceNode.name().equals(originalSourceNode.name())) {
                assertNull(sourceNode.getTimestampExtractor());
            } else {
                assertThat(sourceNode.getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp.class));
            }
        }
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreated() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> kStream = builder.stream("topic-1", stringConsumed);
        final ValueJoiner<String, String, String> valueJoiner = MockValueJoiner.instance(":");
        final long windowSize = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
        final KStream<String, String> stream = kStream
            .map((key, value) -> KeyValue.pair(value, value));
        stream.join(
            kStream,
            valueJoiner,
            JoinWindows.of(ofMillis(windowSize)).grace(ofMillis(3L * windowSize)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        )
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        final ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("X").build();

        final SourceNode originalSourceNode = topology.source("topic-1");

        for (final SourceNode sourceNode : topology.sources()) {
            if (sourceNode.name().equals(originalSourceNode.name())) {
                assertNull(sourceNode.getTimestampExtractor());
            } else {
                assertThat(sourceNode.getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp.class));
            }
        }
    }

    @Test
    public void shouldPropagateRepartitionFlagAfterGlobalKTableJoin() {
        final StreamsBuilder builder = new StreamsBuilder();
        final GlobalKTable<String, String> globalKTable = builder.globalTable("globalTopic");
        final KeyValueMapper<String, String, String> kvMappper = (k, v) -> k + v;
        final ValueJoiner<String, String, String> valueJoiner = (v1, v2) -> v1 + v2;
        builder.<String, String>stream("topic").selectKey((k, v) -> v)
            .join(globalKTable, kvMappper, valueJoiner)
            .groupByKey()
            .count();

        final String topologyDescription = builder.build().describe().toString();

        final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");
        final String topology = builder.build().describe().toString();
        final Matcher matcher = repartitionTopicPattern.matcher(topology);
        assertTrue(matcher.find());
        final String match = matcher.group();
        assertThat(match, notNullValue());
        assertTrue(match.endsWith("repartition"));

    }

    @Test
    public void shouldMergeTwoStreams() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";

        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> merged = source1.merge(source2);

        merged.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic2 =
                driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            inputTopic1.pipeInput("A", "aa");
            inputTopic2.pipeInput("B", "bb");
            inputTopic2.pipeInput("C", "cc");
            inputTopic1.pipeInput("D", "dd");
        }

        assertEquals(asList(new KeyValueTimestamp<>("A", "aa", 0),
            new KeyValueTimestamp<>("B", "bb", 0),
            new KeyValueTimestamp<>("C", "cc", 0),
            new KeyValueTimestamp<>("D", "dd", 0)), processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void shouldMergeMultipleStreams() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";
        final String topic3 = "topic-3";
        final String topic4 = "topic-4";

        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> source3 = builder.stream(topic3);
        final KStream<String, String> source4 = builder.stream(topic4);
        final KStream<String, String> merged = source1.merge(source2).merge(source3).merge(source4);

        merged.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic1 =
                driver.createInputTopic(topic1, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic2 =
                driver.createInputTopic(topic2, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic3 =
                driver.createInputTopic(topic3, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic4 =
                driver.createInputTopic(topic4, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            inputTopic1.pipeInput("A", "aa", 1L);
            inputTopic2.pipeInput("B", "bb", 9L);
            inputTopic3.pipeInput("C", "cc", 2L);
            inputTopic4.pipeInput("D", "dd", 8L);
            inputTopic4.pipeInput("E", "ee", 3L);
            inputTopic3.pipeInput("F", "ff", 7L);
            inputTopic2.pipeInput("G", "gg", 4L);
            inputTopic1.pipeInput("H", "hh", 6L);
        }

        assertEquals(asList(new KeyValueTimestamp<>("A", "aa", 1),
            new KeyValueTimestamp<>("B", "bb", 9),
            new KeyValueTimestamp<>("C", "cc", 2),
            new KeyValueTimestamp<>("D", "dd", 8),
            new KeyValueTimestamp<>("E", "ee", 3),
            new KeyValueTimestamp<>("F", "ff", 7),
            new KeyValueTimestamp<>("G", "gg", 4),
            new KeyValueTimestamp<>("H", "hh", 6)),
            processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void shouldProcessFromSourceThatMatchPattern() {
        final KStream<String, String> pattern2Source = builder.stream(Pattern.compile("topic-\\d"));

        pattern2Source.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic3 =
                driver.createInputTopic("topic-3", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic4 =
                driver.createInputTopic("topic-4", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic5 =
                driver.createInputTopic("topic-5", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic6 =
                driver.createInputTopic("topic-6", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic7 =
                driver.createInputTopic("topic-7", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            inputTopic3.pipeInput("A", "aa", 1L);
            inputTopic4.pipeInput("B", "bb", 5L);
            inputTopic5.pipeInput("C", "cc", 10L);
            inputTopic6.pipeInput("D", "dd", 8L);
            inputTopic7.pipeInput("E", "ee", 3L);
        }

        assertEquals(asList(new KeyValueTimestamp<>("A", "aa", 1),
            new KeyValueTimestamp<>("B", "bb", 5),
            new KeyValueTimestamp<>("C", "cc", 10),
            new KeyValueTimestamp<>("D", "dd", 8),
            new KeyValueTimestamp<>("E", "ee", 3)),
            processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void shouldProcessFromSourcesThatMatchMultiplePattern() {
        final String topic3 = "topic-without-pattern";

        final KStream<String, String> pattern2Source1 = builder.stream(Pattern.compile("topic-\\d"));
        final KStream<String, String> pattern2Source2 = builder.stream(Pattern.compile("topic-[A-Z]"));
        final KStream<String, String> source3 = builder.stream(topic3);
        final KStream<String, String> merged = pattern2Source1.merge(pattern2Source2).merge(source3);

        merged.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic3 =
                driver.createInputTopic("topic-3", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic4 =
                driver.createInputTopic("topic-4", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopicA =
                driver.createInputTopic("topic-A", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopicZ =
                driver.createInputTopic("topic-Z", new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(topic3, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

            inputTopic3.pipeInput("A", "aa", 1L);
            inputTopic4.pipeInput("B", "bb", 5L);
            inputTopicA.pipeInput("C", "cc", 10L);
            inputTopicZ.pipeInput("D", "dd", 8L);
            inputTopic.pipeInput("E", "ee", 3L);
        }

        assertEquals(asList(new KeyValueTimestamp<>("A", "aa", 1),
            new KeyValueTimestamp<>("B", "bb", 5),
            new KeyValueTimestamp<>("C", "cc", 10),
            new KeyValueTimestamp<>("D", "dd", 8),
            new KeyValueTimestamp<>("E", "ee", 3)),
            processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void shouldNotAllowNullTransformerSupplierOnTransform() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transform(null));
        assertThat(exception.getMessage(), equalTo("transformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullTransformerSupplierOnTransformWithStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transform(null, "storeName"));
        assertThat(exception.getMessage(), equalTo("transformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullTransformerSupplierOnTransformWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transform(null, Named.as("transformer")));
        assertThat(exception.getMessage(), equalTo("transformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullTransformerSupplierOnTransformWithNamedAndStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transform(null, Named.as("transformer"), "storeName"));
        assertThat(exception.getMessage(), equalTo("transformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnTransform() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transform(transformerSupplier, (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnTransform() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transform(transformerSupplier, (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnTransformWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transform(transformerSupplier, Named.as("transform"), (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnTransformWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transform(transformerSupplier, Named.as("transform"), (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullNamedOnTransform() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transform(transformerSupplier, (Named) null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnTransformWithStoreName() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transform(transformerSupplier, (Named) null, "storeName"));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullTransformerSupplierOnFlatTransform() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransform(null));
        assertThat(exception.getMessage(), equalTo("transformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullTransformerSupplierOnFlatTransformWithStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransform(null, "storeName"));
        assertThat(exception.getMessage(), equalTo("transformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullTransformerSupplierOnFlatTransformWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransform(null, Named.as("flatTransformer")));
        assertThat(exception.getMessage(), equalTo("transformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullTransformerSupplierOnFlatTransformWithNamedAndStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransform(null, Named.as("flatTransformer"), "storeName"));
        assertThat(exception.getMessage(), equalTo("transformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnFlatTransform() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransform(flatTransformerSupplier, (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnFlatTransform() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransform(flatTransformerSupplier, (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnFlatTransformWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransform(flatTransformerSupplier, Named.as("flatTransform"), (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnFlatTransformWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransform(flatTransformerSupplier, Named.as("flatTransform"), (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullNamedOnFlatTransform() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransform(flatTransformerSupplier, (Named) null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnFlatTransformWithStoreName() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransform(flatTransformerSupplier, (Named) null, "storeName"));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerSupplierOnTransformValues() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues((ValueTransformerSupplier<Object, Object>) null));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerWithKeySupplierOnTransformValues() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues((ValueTransformerWithKeySupplier<Object, Object, Object>) null));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerSupplierOnTransformValuesWithStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                (ValueTransformerSupplier<Object, Object>) null,
                "storeName"));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerWithKeySupplierOnTransformValuesWithStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                (ValueTransformerWithKeySupplier<Object, Object, Object>) null,
                "storeName"));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerSupplierOnTransformValuesWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                (ValueTransformerSupplier<Object, Object>) null,
                Named.as("valueTransformer")));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerWithKeySupplierOnTransformValuesWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                (ValueTransformerWithKeySupplier<Object, Object, Object>) null,
                Named.as("valueTransformerWithKey")));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerSupplierOnTransformValuesWithNamedAndStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                (ValueTransformerSupplier<Object, Object>) null,
                Named.as("valueTransformer"),
                "storeName"));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerWithKeySupplierOnTransformValuesWithNamedAndStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                (ValueTransformerWithKeySupplier<Object, Object, Object>) null,
                Named.as("valueTransformerWithKey"),
                "storeName"));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnTransformValuesWithValueTransformerSupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerSupplier,
                (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnTransformValuesWithValueTransformerWithKeySupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerWithKeySupplier,
                (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnTransformValuesWithValueTransformerSupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerSupplier, (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnTransformValuesWithValueTransformerWithKeySupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerWithKeySupplier,
                (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnTransformValuesWithValueTransformerSupplierWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerSupplier,
                Named.as("valueTransformer"),
                (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnTransformValuesWithValueTransformerWithKeySupplierWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerWithKeySupplier,
                Named.as("valueTransformer"),
                (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnTransformValuesWithValueTransformerSupplierWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerSupplier,
                Named.as("valueTransformer"),
                (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnTransformValuesWithValueTransformerWithKeySupplierWithName() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerWithKeySupplier,
                Named.as("valueTransformerWithKey"),
                (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullNamedOnTransformValuesWithValueTransformerSupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerSupplier,
                (Named) null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnTransformValuesWithValueTransformerWithKeySupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerWithKeySupplier,
                (Named) null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnTransformValuesWithValueTransformerSupplierAndStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerSupplier,
                (Named) null,
                "storeName"));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnTransformValuesWithValueTransformerWithKeySupplierAndStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.transformValues(
                valueTransformerWithKeySupplier,
                (Named) null,
                "storeName"));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerSupplierOnFlatTransformValues() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues((ValueTransformerSupplier<Object, Iterable<Object>>) null));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerWithKeySupplierOnFlatTransformValues() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues((ValueTransformerWithKeySupplier<Object, Object, Iterable<Object>>) null));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerSupplierOnFlatTransformValuesWithStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                (ValueTransformerSupplier<Object, Iterable<Object>>) null,
                "stateStore"));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerWithKeySupplierOnFlatTransformValuesWithStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                (ValueTransformerWithKeySupplier<Object, Object, Iterable<Object>>) null,
                "stateStore"));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerSupplierOnFlatTransformValuesWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                (ValueTransformerSupplier<Object, Iterable<Object>>) null,
                Named.as("flatValueTransformer")));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerWithKeySupplierOnFlatTransformValuesWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                (ValueTransformerWithKeySupplier<Object, Object, Iterable<Object>>) null,
                Named.as("flatValueWithKeyTransformer")));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerSupplierOnFlatTransformValuesWithNamedAndStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                (ValueTransformerSupplier<Object, Iterable<Object>>) null,
                Named.as("flatValueTransformer"),
                "stateStore"));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullValueTransformerWithKeySupplierOnFlatTransformValuesWithNamedAndStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                (ValueTransformerWithKeySupplier<Object, Object, Iterable<Object>>) null,
                Named.as("flatValueWitKeyTransformer"),
                "stateStore"));
        assertThat(exception.getMessage(), equalTo("valueTransformerSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnFlatTransformValuesWithFlatValueSupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerSupplier,
                (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnFlatTransformValuesWithFlatValueWithKeySupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerWithKeySupplier,
                (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnFlatTransformValuesWithFlatValueSupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerSupplier,
                (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnFlatTransformValuesWithFlatValueWithKeySupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerWithKeySupplier,
                (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnFlatTransformValuesWithFlatValueSupplierAndNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerSupplier,
                Named.as("flatValueTransformer"),
                (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnFlatTransformValuesWithFlatValueWithKeySupplierAndNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerWithKeySupplier,
                Named.as("flatValueWitKeyTransformer"),
                (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnFlatTransformValuesWithFlatValueSupplierAndNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerSupplier,
                Named.as("flatValueTransformer"),
                (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnFlatTransformValuesWithFlatValueWithKeySupplierAndNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerWithKeySupplier,
                Named.as("flatValueWitKeyTransformer"),
                (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't contain `null` as store name"));
    }

    @Test
    public void shouldNotAllowNullNamedOnFlatTransformValuesWithFlatValueSupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerSupplier,
                (Named) null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnFlatTransformValuesWithFlatValueWithKeySupplier() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerWithKeySupplier,
                (Named) null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnFlatTransformValuesWithFlatValueSupplierAndStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerSupplier,
                (Named) null,
                "storeName"));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnFlatTransformValuesWithFlatValueWithKeySupplierAndStore() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.flatTransformValues(
                flatValueTransformerWithKeySupplier,
                (Named) null,
                "storeName"));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullProcessSupplierOnProcess() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.process(null));
        assertThat(exception.getMessage(), equalTo("processorSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullProcessSupplierOnProcessWithStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.process(null, "storeName"));
        assertThat(exception.getMessage(), equalTo("processorSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullProcessSupplierOnProcessWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.process(null, Named.as("processor")));
        assertThat(exception.getMessage(), equalTo("processorSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullProcessSupplierOnProcessWithNamedAndStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.process(null, Named.as("processor"), "stateStore"));
        assertThat(exception.getMessage(), equalTo("processorSupplier can't be null"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnProcess() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.process(processorSupplier, (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnProcess() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.process(processorSupplier, (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be null"));
    }

    @Test
    public void shouldNotAllowNullStoreNamesOnProcessWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.process(processorSupplier, Named.as("processor"), (String[]) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be a null array"));
    }

    @Test
    public void shouldNotAllowNullStoreNameOnProcessWithNamed() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.process(processorSupplier, Named.as("processor"), (String) null));
        assertThat(exception.getMessage(), equalTo("stateStoreNames can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnProcess() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.process(processorSupplier, (Named) null));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }

    @Test
    public void shouldNotAllowNullNamedOnProcessWithStores() {
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> testStream.process(processorSupplier, (Named) null, "storeName"));
        assertThat(exception.getMessage(), equalTo("named can't be null"));
    }


    @Test
    public void shouldNotMaterializedKTableFromKStream() {
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());

        final StreamsBuilder builder = new StreamsBuilder();

        final String input = "input";
        final String output = "output";

        builder.stream(input, consumed).toTable().toStream().to(output);

        final String topologyDescription = builder.build().describe().toString();

        assertThat(
            topologyDescription,
            equalTo("Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
                "      --> KSTREAM-TOTABLE-0000000002\n" +
                "    Processor: KSTREAM-TOTABLE-0000000002 (stores: [])\n" +
                "      --> KTABLE-TOSTREAM-0000000003\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n" +
                "    Processor: KTABLE-TOSTREAM-0000000003 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000004\n" +
                "      <-- KSTREAM-TOTABLE-0000000002\n" +
                "    Sink: KSTREAM-SINK-0000000004 (topic: output)\n" +
                "      <-- KTABLE-TOSTREAM-0000000003\n\n")
        );

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, Serdes.String().serializer(), Serdes.String().serializer());
            final TestOutputTopic<String, String> outputTopic =
                driver.createOutputTopic(output, Serdes.String().deserializer(), Serdes.String().deserializer());

            inputTopic.pipeInput("A", "01", 5L);
            inputTopic.pipeInput("B", "02", 100L);
            inputTopic.pipeInput("C", "03", 0L);
            inputTopic.pipeInput("D", "04", 0L);
            inputTopic.pipeInput("A", "05", 10L);
            inputTopic.pipeInput("A", "06", 8L);

            final List<TestRecord<String, String>> outputExpectRecords = new ArrayList<>();
            outputExpectRecords.add(new TestRecord<>("A", "01", Instant.ofEpochMilli(5L)));
            outputExpectRecords.add(new TestRecord<>("B", "02", Instant.ofEpochMilli(100L)));
            outputExpectRecords.add(new TestRecord<>("C", "03", Instant.ofEpochMilli(0L)));
            outputExpectRecords.add(new TestRecord<>("D", "04", Instant.ofEpochMilli(0L)));
            outputExpectRecords.add(new TestRecord<>("A", "05", Instant.ofEpochMilli(10L)));
            outputExpectRecords.add(new TestRecord<>("A", "06", Instant.ofEpochMilli(8L)));

            assertEquals(outputTopic.readRecordsToList(), outputExpectRecords);
        }
    }

    @Test
    public void shouldMaterializeKTableFromKStream() {
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());

        final StreamsBuilder builder = new StreamsBuilder();
        final String storeName = "store";

        final String input = "input";
        builder.stream(input, consumed)
            .toTable(Materialized.as(Stores.inMemoryKeyValueStore(storeName)));

        final String topologyDescription = builder.build().describe().toString();

        assertThat(
            topologyDescription,
            equalTo("Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
                "      --> KSTREAM-TOTABLE-0000000001\n" +
                "    Processor: KSTREAM-TOTABLE-0000000001 (stores: [store])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n\n")
        );

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, Serdes.String().serializer(), Serdes.String().serializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore(storeName);

            inputTopic.pipeInput("A", "01");
            final Map<String, String> expectedStore = mkMap(mkEntry("A", "01"));

            assertThat(asMap(store), is(expectedStore));
        }
    }

    @Test
    public void shouldSupportKeyChangeKTableFromKStream() {
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());

        final StreamsBuilder builder = new StreamsBuilder();

        final String input = "input";
        final String output = "output";

        builder.stream(input, consumed)
            .map((key, value) -> new KeyValue<>(key + "-", value))
            .toTable()
            .toStream().to(output);

        final Topology topology = builder.build();

        final String topologyDescription = topology.describe().toString();

        assertThat(
            topologyDescription,
            equalTo("Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
                "      --> KSTREAM-MAP-0000000001\n" +
                "    Processor: KSTREAM-MAP-0000000001 (stores: [])\n" +
                "      --> KSTREAM-FILTER-0000000005\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n" +
                "    Processor: KSTREAM-FILTER-0000000005 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000004\n" +
                "      <-- KSTREAM-MAP-0000000001\n" +
                "    Sink: KSTREAM-SINK-0000000004 (topic: KSTREAM-TOTABLE-0000000003-repartition)\n" +
                "      <-- KSTREAM-FILTER-0000000005\n" +
                "\n" +
                "  Sub-topology: 1\n" +
                "    Source: KSTREAM-SOURCE-0000000006 (topics: [KSTREAM-TOTABLE-0000000003-repartition])\n" +
                "      --> KSTREAM-TOTABLE-0000000003\n" +
                "    Processor: KSTREAM-TOTABLE-0000000003 (stores: [])\n" +
                "      --> KTABLE-TOSTREAM-0000000007\n" +
                "      <-- KSTREAM-SOURCE-0000000006\n" +
                "    Processor: KTABLE-TOSTREAM-0000000007 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000008\n" +
                "      <-- KSTREAM-TOTABLE-0000000003\n" +
                "    Sink: KSTREAM-SINK-0000000008 (topic: output)\n" +
                "      <-- KTABLE-TOSTREAM-0000000007\n\n")
        );

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, Serdes.String().serializer(), Serdes.String().serializer());
            final TestOutputTopic<String, String> outputTopic =
                driver.createOutputTopic(output, Serdes.String().deserializer(), Serdes.String().deserializer());

            inputTopic.pipeInput("A", "01", 5L);
            inputTopic.pipeInput("B", "02", 100L);
            inputTopic.pipeInput("C", "03", 0L);
            inputTopic.pipeInput("D", "04", 0L);
            inputTopic.pipeInput("A", "05", 10L);
            inputTopic.pipeInput("A", "06", 8L);

            final List<TestRecord<String, String>> outputExpectRecords = new ArrayList<>();
            outputExpectRecords.add(new TestRecord<>("A-", "01", Instant.ofEpochMilli(5L)));
            outputExpectRecords.add(new TestRecord<>("B-", "02", Instant.ofEpochMilli(100L)));
            outputExpectRecords.add(new TestRecord<>("C-", "03", Instant.ofEpochMilli(0L)));
            outputExpectRecords.add(new TestRecord<>("D-", "04", Instant.ofEpochMilli(0L)));
            outputExpectRecords.add(new TestRecord<>("A-", "05", Instant.ofEpochMilli(10L)));
            outputExpectRecords.add(new TestRecord<>("A-", "06", Instant.ofEpochMilli(8L)));

            assertEquals(outputTopic.readRecordsToList(), outputExpectRecords);
        }
    }


    @Test
    public void shouldSupportForeignKeyTableTableJoinWithKTableFromKStream() {
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final StreamsBuilder builder = new StreamsBuilder();

        final String input1 = "input1";
        final String input2 = "input2";
        final String output = "output";

        final KTable<String, String> leftTable = builder.stream(input1, consumed).toTable();
        final KTable<String, String> rightTable = builder.stream(input2, consumed).toTable();

        final Function<String, String> extractor = value -> value.split("\\|")[1];
        final ValueJoiner<String, String, String> joiner = (value1, value2) -> "(" + value1 + "," + value2 + ")";

        leftTable.join(rightTable, extractor, joiner).toStream().to(output);

        final Topology topology = builder.build(props);

        final String topologyDescription = topology.describe().toString();

        assertThat(
            topologyDescription,
            equalTo("Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KTABLE-SOURCE-0000000016 (topics: [KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000014-topic])\n" +
                "      --> KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-RESOLVER-PROCESSOR-0000000017\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input1])\n" +
                "      --> KSTREAM-TOTABLE-0000000002\n" +
                "    Processor: KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-RESOLVER-PROCESSOR-0000000017 (stores: [KSTREAM-TOTABLE-STATE-STORE-0000000001])\n" +
                "      --> KTABLE-FK-JOIN-OUTPUT-0000000018\n" +
                "      <-- KTABLE-SOURCE-0000000016\n" +
                "    Processor: KSTREAM-TOTABLE-0000000002 (stores: [KSTREAM-TOTABLE-STATE-STORE-0000000001])\n" +
                "      --> KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000007\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n" +
                "    Processor: KTABLE-FK-JOIN-OUTPUT-0000000018 (stores: [])\n" +
                "      --> KTABLE-TOSTREAM-0000000020\n" +
                "      <-- KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-RESOLVER-PROCESSOR-0000000017\n" +
                "    Processor: KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000007 (stores: [])\n" +
                "      --> KTABLE-SINK-0000000008\n" +
                "      <-- KSTREAM-TOTABLE-0000000002\n" +
                "    Processor: KTABLE-TOSTREAM-0000000020 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000021\n" +
                "      <-- KTABLE-FK-JOIN-OUTPUT-0000000018\n" +
                "    Sink: KSTREAM-SINK-0000000021 (topic: output)\n" +
                "      <-- KTABLE-TOSTREAM-0000000020\n" +
                "    Sink: KTABLE-SINK-0000000008 (topic: KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic)\n" +
                "      <-- KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000007\n" +
                "\n" +
                "  Sub-topology: 1\n" +
                "    Source: KSTREAM-SOURCE-0000000003 (topics: [input2])\n" +
                "      --> KSTREAM-TOTABLE-0000000005\n" +
                "    Source: KTABLE-SOURCE-0000000009 (topics: [KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic])\n" +
                "      --> KTABLE-FK-JOIN-SUBSCRIPTION-PROCESSOR-0000000011\n" +
                "    Processor: KSTREAM-TOTABLE-0000000005 (stores: [KSTREAM-TOTABLE-STATE-STORE-0000000004])\n" +
                "      --> KTABLE-FK-JOIN-SUBSCRIPTION-PROCESSOR-0000000013\n" +
                "      <-- KSTREAM-SOURCE-0000000003\n" +
                "    Processor: KTABLE-FK-JOIN-SUBSCRIPTION-PROCESSOR-0000000011 (stores: [KTABLE-FK-JOIN-SUBSCRIPTION-STATE-STORE-0000000010])\n" +
                "      --> KTABLE-FK-JOIN-SUBSCRIPTION-PROCESSOR-0000000012\n" +
                "      <-- KTABLE-SOURCE-0000000009\n" +
                "    Processor: KTABLE-FK-JOIN-SUBSCRIPTION-PROCESSOR-0000000012 (stores: [KSTREAM-TOTABLE-STATE-STORE-0000000004])\n" +
                "      --> KTABLE-SINK-0000000015\n" +
                "      <-- KTABLE-FK-JOIN-SUBSCRIPTION-PROCESSOR-0000000011\n" +
                "    Processor: KTABLE-FK-JOIN-SUBSCRIPTION-PROCESSOR-0000000013 (stores: [KTABLE-FK-JOIN-SUBSCRIPTION-STATE-STORE-0000000010])\n" +
                "      --> KTABLE-SINK-0000000015\n" +
                "      <-- KSTREAM-TOTABLE-0000000005\n" +
                "    Sink: KTABLE-SINK-0000000015 (topic: KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000014-topic)\n" +
                "      <-- KTABLE-FK-JOIN-SUBSCRIPTION-PROCESSOR-0000000012, KTABLE-FK-JOIN-SUBSCRIPTION-PROCESSOR-0000000013\n\n")
        );


        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> left = driver.createInputTopic(input1, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> right = driver.createInputTopic(input2, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(output, new StringDeserializer(), new StringDeserializer());

            // Pre-populate the RHS records. This test is all about what happens when we add/remove LHS records
            right.pipeInput("rhs1", "rhsValue1");
            right.pipeInput("rhs2", "rhsValue2");
            right.pipeInput("rhs3", "rhsValue3"); // this unreferenced FK won't show up in any results

            assertThat(outputTopic.readKeyValuesToMap(), is(emptyMap()));

            left.pipeInput("lhs1", "lhsValue1|rhs1");
            left.pipeInput("lhs2", "lhsValue2|rhs2");

            final Map<String, String> expected = mkMap(
                mkEntry("lhs1", "(lhsValue1|rhs1,rhsValue1)"),
                mkEntry("lhs2", "(lhsValue2|rhs2,rhsValue2)")
            );
            assertThat(outputTopic.readKeyValuesToMap(), is(expected));

            // Add another reference to an existing FK
            left.pipeInput("lhs3", "lhsValue3|rhs1");

            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs3", "(lhsValue3|rhs1,rhsValue1)")
                ))
            );

            left.pipeInput("lhs1", (String) null);
            assertThat(
                outputTopic.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs1", null)
                ))
            );
        }
    }

    @Test
    public void shouldSupportTableTableJoinWithKStreamToKTable() {
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final StreamsBuilder builder = new StreamsBuilder();

        final String leftTopic = "left";
        final String rightTopic = "right";
        final String outputTopic = "output";

        final KTable<String, String> table1 = builder.stream(leftTopic, consumed).toTable();
        final KTable<String, String> table2 = builder.stream(rightTopic, consumed).toTable();

        table1.join(table2, MockValueJoiner.TOSTRING_JOINER).toStream().to(outputTopic);

        final Topology topology = builder.build(props);
        final String topologyDescription = topology.describe().toString();

        assertThat(
            topologyDescription,
            equalTo("Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [left])\n" +
                "      --> KSTREAM-TOTABLE-0000000002\n" +
                "    Source: KSTREAM-SOURCE-0000000003 (topics: [right])\n" +
                "      --> KSTREAM-TOTABLE-0000000005\n" +
                "    Processor: KSTREAM-TOTABLE-0000000002 (stores: [KSTREAM-TOTABLE-STATE-STORE-0000000001])\n" +
                "      --> KTABLE-JOINTHIS-0000000007\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n" +
                "    Processor: KSTREAM-TOTABLE-0000000005 (stores: [KSTREAM-TOTABLE-STATE-STORE-0000000004])\n" +
                "      --> KTABLE-JOINOTHER-0000000008\n" +
                "      <-- KSTREAM-SOURCE-0000000003\n" +
                "    Processor: KTABLE-JOINOTHER-0000000008 (stores: [KSTREAM-TOTABLE-STATE-STORE-0000000001])\n" +
                "      --> KTABLE-MERGE-0000000006\n" +
                "      <-- KSTREAM-TOTABLE-0000000005\n" +
                "    Processor: KTABLE-JOINTHIS-0000000007 (stores: [KSTREAM-TOTABLE-STATE-STORE-0000000004])\n" +
                "      --> KTABLE-MERGE-0000000006\n" +
                "      <-- KSTREAM-TOTABLE-0000000002\n" +
                "    Processor: KTABLE-MERGE-0000000006 (stores: [])\n" +
                "      --> KTABLE-TOSTREAM-0000000009\n" +
                "      <-- KTABLE-JOINTHIS-0000000007, KTABLE-JOINOTHER-0000000008\n" +
                "    Processor: KTABLE-TOSTREAM-0000000009 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000010\n" +
                "      <-- KTABLE-MERGE-0000000006\n" +
                "    Sink: KSTREAM-SINK-0000000010 (topic: output)\n" +
                "      <-- KTABLE-TOSTREAM-0000000009\n\n"));

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(topology).copartitionGroups();

        assertEquals(1, copartitionGroups.size());

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> left = driver.createInputTopic(leftTopic, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> right = driver.createInputTopic(rightTopic, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> output = driver.createOutputTopic(outputTopic, new StringDeserializer(), new StringDeserializer());

            right.pipeInput("lhs1", "rhsValue1");
            right.pipeInput("rhs2", "rhsValue2");
            right.pipeInput("lhs3", "rhsValue3");

            assertThat(output.readKeyValuesToMap(), is(emptyMap()));

            left.pipeInput("lhs1", "lhsValue1");
            left.pipeInput("lhs2", "lhsValue2");

            final Map<String, String> expected = mkMap(
                mkEntry("lhs1", "lhsValue1+rhsValue1")
            );

            assertThat(
                output.readKeyValuesToMap(),
                is(expected)
            );

            left.pipeInput("lhs3", "lhsValue3");

            assertThat(
                output.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs3", "lhsValue3+rhsValue3")
                ))
            );

            left.pipeInput("lhs1", "lhsValue4");
            assertThat(
                output.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs1", "lhsValue4+rhsValue1")
                ))
            );

        }
    }

    @Test
    public void shouldSupportStreamTableJoinWithKStreamToKTable() {
        final String streamTopic = "streamTopic";
        final String tableTopic = "tableTopic";
        final String outputTopic = "output";
        final StreamsBuilder builder = new StreamsBuilder();
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());

        final KStream<String, String> stream = builder.stream(streamTopic, consumed);
        final KTable<String, String> table =  builder.stream(tableTopic, consumed).toTable();

        stream.join(table, MockValueJoiner.TOSTRING_JOINER).to(outputTopic);

        final Topology topology = builder.build(props);
        final String topologyDescription = topology.describe().toString();

        assertThat(
            topologyDescription,
            equalTo("Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [streamTopic])\n" +
                "      --> KSTREAM-JOIN-0000000004\n" +
                "    Processor: KSTREAM-JOIN-0000000004 (stores: [KSTREAM-TOTABLE-STATE-STORE-0000000002])\n" +
                "      --> KSTREAM-SINK-0000000005\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n" +
                "    Source: KSTREAM-SOURCE-0000000001 (topics: [tableTopic])\n" +
                "      --> KSTREAM-TOTABLE-0000000003\n" +
                "    Sink: KSTREAM-SINK-0000000005 (topic: output)\n" +
                "      <-- KSTREAM-JOIN-0000000004\n" +
                "    Processor: KSTREAM-TOTABLE-0000000003 (stores: [KSTREAM-TOTABLE-STATE-STORE-0000000002])\n" +
                "      --> none\n" +
                "      <-- KSTREAM-SOURCE-0000000001\n\n"));

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(topology).copartitionGroups();

        assertEquals(1, copartitionGroups.size());

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> left = driver.createInputTopic(streamTopic, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> right = driver.createInputTopic(tableTopic, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> output = driver.createOutputTopic(outputTopic, new StringDeserializer(), new StringDeserializer());

            right.pipeInput("lhs1", "rhsValue1");
            right.pipeInput("rhs2", "rhsValue2");
            right.pipeInput("lhs3", "rhsValue3");

            assertThat(output.readKeyValuesToMap(), is(emptyMap()));

            left.pipeInput("lhs1", "lhsValue1");
            left.pipeInput("lhs2", "lhsValue2");

            final Map<String, String> expected = mkMap(
                mkEntry("lhs1", "lhsValue1+rhsValue1")
            );

            assertThat(
                output.readKeyValuesToMap(),
                is(expected)
            );

            left.pipeInput("lhs3", "lhsValue3");

            assertThat(
                output.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs3", "lhsValue3+rhsValue3")
                ))
            );

            left.pipeInput("lhs1", "lhsValue4");
            assertThat(
                output.readKeyValuesToMap(),
                is(mkMap(
                    mkEntry("lhs1", "lhsValue4+rhsValue1")
                ))
            );

        }
    }

    @Test
    public void shouldSupportGroupByCountWithKStreamToKTable() {
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "input";
        final String output = "output";

        builder
            .stream(input, consumed)
            .toTable()
            .groupBy(MockMapper.selectValueKeyValueMapper(), Grouped.with(Serdes.String(), Serdes.String()))
            .count()
            .toStream()
            .to(output);

        final Topology topology = builder.build(props);
        final String topologyDescription = topology.describe().toString();

        assertThat(
            topologyDescription,
            equalTo("Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
                "      --> KSTREAM-TOTABLE-0000000002\n" +
                "    Processor: KSTREAM-TOTABLE-0000000002 (stores: [KSTREAM-TOTABLE-STATE-STORE-0000000001])\n" +
                "      --> KTABLE-SELECT-0000000003\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n" +
                "    Processor: KTABLE-SELECT-0000000003 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000005\n" +
                "      <-- KSTREAM-TOTABLE-0000000002\n" +
                "    Sink: KSTREAM-SINK-0000000005 (topic: KTABLE-AGGREGATE-STATE-STORE-0000000004-repartition)\n" +
                "      <-- KTABLE-SELECT-0000000003\n" +
                "\n" +
                "  Sub-topology: 1\n" +
                "    Source: KSTREAM-SOURCE-0000000006 (topics: [KTABLE-AGGREGATE-STATE-STORE-0000000004-repartition])\n" +
                "      --> KTABLE-AGGREGATE-0000000007\n" +
                "    Processor: KTABLE-AGGREGATE-0000000007 (stores: [KTABLE-AGGREGATE-STATE-STORE-0000000004])\n" +
                "      --> KTABLE-TOSTREAM-0000000008\n" +
                "      <-- KSTREAM-SOURCE-0000000006\n" +
                "    Processor: KTABLE-TOSTREAM-0000000008 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000009\n" +
                "      <-- KTABLE-AGGREGATE-0000000007\n" +
                "    Sink: KSTREAM-SINK-0000000009 (topic: output)\n" +
                "      <-- KTABLE-TOSTREAM-0000000008\n\n"));

        try (
            final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestOutputTopic<String, Long> outputTopic =
                driver.createOutputTopic(output, Serdes.String().deserializer(), Serdes.Long().deserializer());


            inputTopic.pipeInput("A", "green", 10L);
            inputTopic.pipeInput("B", "green", 9L);
            inputTopic.pipeInput("A", "blue", 12L);
            inputTopic.pipeInput("C", "yellow", 15L);
            inputTopic.pipeInput("D", "green", 11L);

            assertEquals(
                asList(
                    new TestRecord<>("green", 1L, Instant.ofEpochMilli(10)),
                    new TestRecord<>("green", 2L, Instant.ofEpochMilli(10)),
                    new TestRecord<>("green", 1L, Instant.ofEpochMilli(12)),
                    new TestRecord<>("blue", 1L, Instant.ofEpochMilli(12)),
                    new TestRecord<>("yellow", 1L, Instant.ofEpochMilli(15)),
                    new TestRecord<>("green", 2L, Instant.ofEpochMilli(12))),
                outputTopic.readRecordsToList());
        }
    }

    @Test
    public void shouldSupportTriggerMaterializedWithKTableFromKStream() {
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "input";
        final String output = "output";
        final String storeName = "store";

        builder.stream(input, consumed)
            .toTable()
            .mapValues(
                value -> String.valueOf(value.charAt(0) - (int) 'a'),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(storeName)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String()))
            .toStream()
            .to(output);

        final Topology topology = builder.build(props);
        final String topologyDescription = topology.describe().toString();

        assertThat(
            topologyDescription,
            equalTo("Topologies:\n" +
                "   Sub-topology: 0\n" +
                "    Source: KSTREAM-SOURCE-0000000000 (topics: [input])\n" +
                "      --> KSTREAM-TOTABLE-0000000002\n" +
                "    Processor: KSTREAM-TOTABLE-0000000002 (stores: [])\n" +
                "      --> KTABLE-MAPVALUES-0000000003\n" +
                "      <-- KSTREAM-SOURCE-0000000000\n" +
                "    Processor: KTABLE-MAPVALUES-0000000003 (stores: [store])\n" +
                "      --> KTABLE-TOSTREAM-0000000004\n" +
                "      <-- KSTREAM-TOTABLE-0000000002\n" +
                "    Processor: KTABLE-TOSTREAM-0000000004 (stores: [])\n" +
                "      --> KSTREAM-SINK-0000000005\n" +
                "      <-- KTABLE-MAPVALUES-0000000003\n" +
                "    Sink: KSTREAM-SINK-0000000005 (topic: output)\n" +
                "      <-- KTABLE-TOSTREAM-0000000004\n\n"));

        try (
            final TopologyTestDriver driver = new TopologyTestDriver(
                topology,
                mkProperties(mkMap(
                    mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy"),
                    mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test"),
                    mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory("kafka-test").getAbsolutePath())
                )),
                Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(input, new StringSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestOutputTopic<String, String> outputTopic =
                driver.createOutputTopic(output, Serdes.String().deserializer(), Serdes.String().deserializer());
            final KeyValueStore<String, String> store = driver.getKeyValueStore(storeName);

            inputTopic.pipeInput("A", "green", 10L);
            inputTopic.pipeInput("B", "green", 9L);
            inputTopic.pipeInput("A", "blue", 12L);
            inputTopic.pipeInput("C", "yellow", 15L);
            inputTopic.pipeInput("D", "green", 11L);

            final Map<String, String> expectedStore = new HashMap<>();
            expectedStore.putIfAbsent("A", "1");
            expectedStore.putIfAbsent("B", "6");
            expectedStore.putIfAbsent("C", "24");
            expectedStore.putIfAbsent("D", "6");

            assertEquals(expectedStore, asMap(store));

            assertEquals(
                asList(
                    new TestRecord<>("A", "6", Instant.ofEpochMilli(10)),
                    new TestRecord<>("B", "6", Instant.ofEpochMilli(9)),
                    new TestRecord<>("A", "1", Instant.ofEpochMilli(12)),
                    new TestRecord<>("C", "24", Instant.ofEpochMilli(15)),
                    new TestRecord<>("D", "6", Instant.ofEpochMilli(11))),
                outputTopic.readRecordsToList());

        }
    }

    private static Map<String, String> asMap(final KeyValueStore<String, String> store) {
        final HashMap<String, String> result = new HashMap<>();
        store.all().forEachRemaining(kv -> {
            System.out.println(kv);
            result.put(kv.key, kv.value);
        });
        return result;
    }
}
