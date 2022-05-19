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
package org.apache.kafka.streams.integration;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.KeyValue.pair;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForApplicationState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({IntegrationTest.class})
public class PauseResumeIntegrationTest {
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(45);
    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private static Properties producerConfig;
    private static Properties consumerConfig;

    private static final Materialized<Object, Long, KeyValueStore<Bytes, byte[]>> IN_MEMORY_STORE =
        Materialized.as(Stores.inMemoryKeyValueStore("store"));

    private static final String INPUT_STREAM_1 = "input-stream-1";
    private static final String INPUT_STREAM_2 = "input-stream-2";

    private static final String OUTPUT_STREAM_1 = "output-stream-1";
    private static final String OUTPUT_STREAM_2 = "output-stream-2";
    
    private static final String TOPOLOGY1 = "topology1";
    private static final String TOPOLOGY2 = "topology2";

    private static final List<KeyValue<String, Long>> STANDARD_INPUT_DATA =
        asList(pair("A", 100L), pair("B", 200L), pair("A", 300L), pair("C", 400L), pair("C", -50L));
    private static final List<KeyValue<String, Long>> COUNT_OUTPUT_DATA =
        asList(pair("B", 1L), pair("A", 2L), pair("C", 2L));
    private static final List<KeyValue<String, Long>> COUNT_OUTPUT_DATA2 =
        asList(pair("B", 2L), pair("A", 4L), pair("C", 4L));

    @BeforeClass
    public static void startCluster() throws Exception {
        CLUSTER.start();

        producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(),
            StringSerializer.class, LongSerializer.class);
        consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(),
            StringDeserializer.class, LongDeserializer.class);
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Rule
    public final TestName testName = new TestName();
    private String appId;
    private KafkaStreams kafkaStreams;

    @Before
    public void createTopics() throws InterruptedException {
        appId = safeUniqueTestName(PauseResumeIntegrationTest.class, testName);

        CLUSTER.createTopic(INPUT_STREAM_1, 2, 1);
        CLUSTER.createTopic(INPUT_STREAM_2, 2, 1);
        CLUSTER.createTopic(OUTPUT_STREAM_1, 2, 1);
        CLUSTER.createTopic(OUTPUT_STREAM_2, 2, 1);
    }

    private Properties props() {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        properties.put(StreamsConfig.STATE_DIR_CONFIG,
            TestUtils.tempDirectory(appId).getPath());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        return properties;
    }

    @After
    public void shutdown() throws InterruptedException {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30));
        }
        CLUSTER.deleteTopicsAndWait(INPUT_STREAM_1);
        CLUSTER.deleteTopicsAndWait(INPUT_STREAM_2);
        CLUSTER.deleteTopicsAndWait(OUTPUT_STREAM_1);
        CLUSTER.deleteTopicsAndWait(OUTPUT_STREAM_2);
    }

    private static void produceToInputTopics(final String topic,
        final Collection<KeyValue<String, Long>> records) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            topic,
            records,
            producerConfig,
            CLUSTER.time
        );
    }


    @Test
    public void shouldPauseAndResumeKafkaStreams() throws Exception {
        // Create KafkaStreams instance
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_STREAM_1).groupByKey().count().toStream().to(OUTPUT_STREAM_1);

        kafkaStreams = new KafkaStreams(builder.build(props()), props());
        kafkaStreams.start();
        waitForApplicationState(singletonList(kafkaStreams), State.RUNNING, STARTUP_TIMEOUT);

        // Write data
        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);

        // Verify output
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA));

        // Pause KafkaStreams/topology
        kafkaStreams.pause();
        assertTrue(kafkaStreams.isPaused());

        // Write more data
        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);

        // Verify that consumers read new data -- AKA, there is no lag.
        final Map<String, Map<Integer, LagInfo>> lagMap = kafkaStreams.allLocalStorePartitionLags();
        assertNoLag(lagMap);

        // Verify no output somehow?
        // Is there a better way to show this?
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 0),
            CoreMatchers.equalTo(Collections.emptyList()));

        // Resume KafkaStreams/topology
        kafkaStreams.resume();
        assertFalse(kafkaStreams.isPaused());

        // Verify that the input written during the pause is processed.
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA2));
    }

    @Test
    public void shouldAllowForTopologiesToStartPaused() throws Exception {
        // Create KafkaStreams instance
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_STREAM_1).groupByKey().count().toStream().to(OUTPUT_STREAM_1);

        kafkaStreams = new KafkaStreams(builder.build(props()), props());

        // Start KafkaStream with paused processing.
        kafkaStreams.pause();
        kafkaStreams.start();
        // Check for rebalancing instead?
        waitForApplicationState(singletonList(kafkaStreams), State.RUNNING, STARTUP_TIMEOUT);

        assertTrue(kafkaStreams.isPaused());

        // Write data
        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);

        // Verify that consumers read new data -- AKA, there is no lag.
        /*
        final Map<String, Map<Integer, LagInfo>> lagMap =
            kafkaStreams.allLocalStorePartitionLags();
        assertNoLag(lagMap);
        */

        // Verify no output
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 0),
            CoreMatchers.equalTo(Collections.emptyList()));

        // Resume KafkaStreams/topology
        kafkaStreams.resume();
        assertFalse(kafkaStreams.isPaused());

        // Verify that the input written during the pause is processed.
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA));
    }

    @Test
    public void shouldPauseAndResumeKafkaStreamsWithNamedTopologies() throws Exception {
        // Create KafkaStream / NamedTopologyBuilders
        final KafkaStreamsNamedTopologyWrapper streams =
            new KafkaStreamsNamedTopologyWrapper(props());
        final NamedTopologyBuilder builder1 = streams.newNamedTopologyBuilder(TOPOLOGY1);
        builder1.stream(INPUT_STREAM_1).groupByKey().count().toStream().to(OUTPUT_STREAM_1);

        final NamedTopologyBuilder builder2 = streams.newNamedTopologyBuilder(TOPOLOGY2);
        builder2.stream(INPUT_STREAM_2)
            .groupBy((k, v) -> k)
            .count(IN_MEMORY_STORE)
            .toStream()
            .to(OUTPUT_STREAM_2);

        // Start KafkaStreams instance
        streams.start(asList(builder1.build(), builder2.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);

        // Write data for topology1
        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);

        // Verify output
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA));

        // Pause topology1
        streams.pauseNamedTopology(TOPOLOGY1);

        // Assert the topology1 is paused and topology2 is not paused
        assertTrue(streams.isNamedTopologyPaused(TOPOLOGY1));
        assertFalse(streams.isNamedTopologyPaused(TOPOLOGY2));
        assertFalse(streams.isPaused());

        // Write more data for topology 1 and topology 2
        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);

        // Verify that consumers read new data -- AKA, there is no lag.
        final Map<String, Map<Integer, LagInfo>> lagMap = streams.allLocalStorePartitionLags();
        assertNoLag(lagMap);

        // Verify no output for topology1 somehow? (Hard to prove negative)
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 0),
            CoreMatchers.equalTo(Collections.emptyList()));

        // Verify that topology 2 is not paused and has processed data.
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA2));


        // Resume topology1 and show that it processes data.
        streams.resumeNamedTopology(TOPOLOGY1);
        assertFalse(streams.isNamedTopologyPaused(TOPOLOGY1));

        // Verify that the input written during the pause is processed.
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA2));
    }

    @Test
    public void shouldPauseAndResumeAllKafkaStreamsWithNamedTopologies() throws Exception {
        // Create KafkaStreams instance / NamedTopologyBuilders
        final KafkaStreamsNamedTopologyWrapper streams = new KafkaStreamsNamedTopologyWrapper(props());
        final NamedTopologyBuilder builder1 = streams.newNamedTopologyBuilder(TOPOLOGY1);
        builder1.stream(INPUT_STREAM_1).groupByKey().count().toStream().to(OUTPUT_STREAM_1);

        final NamedTopologyBuilder builder2 = streams.newNamedTopologyBuilder(TOPOLOGY2);
        builder2.stream(INPUT_STREAM_2)
            .groupBy((k, v) -> k)
            .count(IN_MEMORY_STORE)
            .toStream()
            .to(OUTPUT_STREAM_2);

        // Start KafkaStream
        streams.start(asList(builder1.build(), builder2.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);

        // Write data for topology1
        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);

        // Verify output
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA));

        // Pause KafkaStreams instance.  This will pause all named/module topologies.
        streams.pause();
        assertTrue(streams.isPaused());
        assertTrue(streams.isNamedTopologyPaused(TOPOLOGY1));
        assertTrue(streams.isNamedTopologyPaused(TOPOLOGY2));


        // Write more data for topology 1 and topology 2
        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);

        // Verify that consumers read new data -- AKA, there is no lag.
        final Map<String, Map<Integer, LagInfo>> lagMap = streams.allLocalStorePartitionLags();
        assertNoLag(lagMap);

        // Verify no output for topology1 somehow? (Hard to prove negative)
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 0),
            CoreMatchers.equalTo(Collections.emptyList()));

        // Verify that topology 2 is paused and has processed data.
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 0),
            CoreMatchers.equalTo(Collections.emptyList()));

        // Resume topology1.  This will cause us to see that the instance is not paused since there
        // is at least one topology being processed.
        streams.resumeNamedTopology(TOPOLOGY1);
        assertFalse(streams.isPaused());
        assertFalse(streams.isNamedTopologyPaused(TOPOLOGY1));
        assertTrue(streams.isNamedTopologyPaused(TOPOLOGY2));  // Still paused

        // Verify that the input written during the pause is processed.
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA2));
    }

    @Test
    public void shouldAllowForNamedTopologiesToStartPaused() throws Exception {
        // Create KafkaStreams instance / NamedTopologyBuilders
        final KafkaStreamsNamedTopologyWrapper streams = new KafkaStreamsNamedTopologyWrapper(props());
        final NamedTopologyBuilder builder1 = streams.newNamedTopologyBuilder(TOPOLOGY1);
        builder1.stream(INPUT_STREAM_1).groupByKey().count().toStream().to(OUTPUT_STREAM_1);

        final NamedTopologyBuilder builder2 = streams.newNamedTopologyBuilder(TOPOLOGY2);
        builder2.stream(INPUT_STREAM_2)
            .groupBy((k, v) -> k)
            .count(IN_MEMORY_STORE)
            .toStream()
            .to(OUTPUT_STREAM_2);

        // Start KafkaStream with topology1 paused.
        streams.pauseNamedTopology(TOPOLOGY1);
        streams.start(asList(builder1.build(), builder2.build()));
        waitForApplicationState(singletonList(streams), State.RUNNING, STARTUP_TIMEOUT);

        assertFalse(streams.isPaused());
        assertTrue(streams.isNamedTopologyPaused(TOPOLOGY1));
        assertFalse(streams.isNamedTopologyPaused(TOPOLOGY2));

        // Write data for topology1
        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);

        // Verify output
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 0),
            CoreMatchers.equalTo(Collections.emptyList()));
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA));

        // Pause KafkaStreams instance
        streams.pause();

        assertTrue(streams.isPaused());
        assertTrue(streams.isNamedTopologyPaused(TOPOLOGY1));
        assertTrue(streams.isNamedTopologyPaused(TOPOLOGY2));

        // Write more data for topology 1 and topology 2
        produceToInputTopics(INPUT_STREAM_1, STANDARD_INPUT_DATA);
        produceToInputTopics(INPUT_STREAM_2, STANDARD_INPUT_DATA);

        // Verify that consumers read new data -- AKA, there is no lag.
        final Map<String, Map<Integer, LagInfo>> lagMap = streams.allLocalStorePartitionLags();
        assertNoLag(lagMap);

        // Verify no output for topology1 somehow? (Hard to prove negative)
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 0),
            CoreMatchers.equalTo(Collections.emptyList()));

        // Verify that topology 2 is paused and has not processed data.
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_2, 0),
            CoreMatchers.equalTo(Collections.emptyList()));

        // Resume topology 1; assert that it is running.
        streams.resumeNamedTopology(TOPOLOGY1);
        assertFalse(streams.isPaused()); // Are all paused?  No.
        assertFalse(streams.isNamedTopologyPaused(TOPOLOGY1));
        assertTrue(streams.isNamedTopologyPaused(TOPOLOGY2));  // Still paused

        // Verify that the input written during the pause is processed.
        assertThat(waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_STREAM_1, 3),
            CoreMatchers.equalTo(COUNT_OUTPUT_DATA2));
    }

    private static void assertNoLag(final Map<String, Map<Integer, LagInfo>> lagMap) {
        final Long maxLag = lagMap.values()
            .stream()
            .flatMap(m -> m.values().stream())
            .map(LagInfo::offsetLag)
            .max(Long::compare).get();
        assertEquals(0, (long) maxLag);
    }
}
