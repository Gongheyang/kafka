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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.test.TestUtils.waitForCondition;

/**
 * An integration test to verify the conversion of a dirty-closed EOS
 * task towards a standby task is safe across restarts of the application.
 */
public class StandbyTaskEOSIntegrationTest {

    private final AtomicBoolean skip = new AtomicBoolean(false);

    private String appId;
    private String inputTopic;
    private String storeName;
    private String outputTopic;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @Before
    public void createTopics() throws Exception {
        appId = "standbyTest";
        inputTopic = "testInputTopic";
        outputTopic = "testOutputTopic";
        storeName = "dedupStore";
        CLUSTER.deleteTopicsAndWait(inputTopic, outputTopic);
        CLUSTER.createTopic(inputTopic, 1, 3);
        CLUSTER.createTopic(outputTopic, 1, 3);
    }

    @Test
    public void shouldWipeOutStandbyStateDirectoryIfCheckpointIsMissing() throws Exception {
        final String base = TestUtils.tempDirectory(appId).getPath();

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic,
            Collections.singletonList(
                new KeyValue<>(0, 0)
            ),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class,
                new Properties()
            ),
            10L
        );

        try (
            final KafkaStreams streamInstanceOne = buildWithDeduplicationTopology(base + "-1");
            final KafkaStreams streamInstanceTwo = buildWithDeduplicationTopology(base + "-2");
            final KafkaStreams streamInstanceOneRecovery = buildWithDeduplicationTopology(base + "-1")
        ) {
            // start first instance and wait for processing
            startApplicationAndWaitUntilRunning(Collections.singletonList(streamInstanceOne), Duration.ofSeconds(30));
            IntegrationTestUtils.waitUntilMinRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerDeserializer.class,
                    IntegerDeserializer.class
                ),
                outputTopic,
                1
            );

            // start second instance and wait for standby replication
            startApplicationAndWaitUntilRunning(Collections.singletonList(streamInstanceTwo), Duration.ofSeconds(30));
            waitForCondition(
                () -> streamInstanceTwo.store(
                    StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.<Integer, Integer>keyValueStore()
                    ).enableStaleStores()
                ).get(0) != null,
                120_000L, // use increased timeout to encounter for rebalancing time
                "Could not get key from standby store"
            );
            // sanity check that first instance is still active
            waitForCondition(
                () -> streamInstanceOne.store(
                    StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.<Integer, Integer>keyValueStore()
                    )
                ).get(0) != null,
                "Could not get key from main store"
            );

            // inject poison pill and wait for crash of first instance and recovery on second instance
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                inputTopic,
                Collections.singletonList(
                    new KeyValue<>(1, 0)
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerSerializer.class,
                    IntegerSerializer.class,
                    new Properties()
                ),
                10L
            );
            waitForCondition(
                () -> streamInstanceOne.state() == KafkaStreams.State.ERROR,
                "Stream instance 1 did not go into error state"
            );
            streamInstanceOne.close();

            waitForCondition(
                () -> streamInstanceTwo.store(
                    StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.keyValueStore()
                    )
                ).get(0) != null,
                120_000L, // use increased timeout to encounter for rebalancing time
                "Could not get key from recovered main store"
            );

            // "restart" first client and wait for standby recovery
            startApplicationAndWaitUntilRunning(
                Collections.singletonList(streamInstanceOneRecovery),
                Duration.ofSeconds(30)
            );
            waitForCondition(
                () -> streamInstanceOneRecovery.store(
                    StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.<Integer, Integer>keyValueStore()
                    ).enableStaleStores()
                ).get(0) != null,
                "Could not get key from recovered standby store"
            );
            // sanity check that second instance is still active
            waitForCondition(
                () -> streamInstanceTwo.store(
                    StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.keyValueStore()
                    )
                ).get(0) != null,
                "Could not get key from recovered main store"
            );

            streamInstanceTwo.close();
            waitForCondition(
                () -> streamInstanceOneRecovery.store(
                    StoreQueryParameters.fromNameAndType(
                        storeName,
                        QueryableStoreTypes.<Integer, Integer>keyValueStore()
                    )
                ).get(0) != null,
                120_000L, // use increased timeout to encounter for rebalancing time
                "Could not get key from recovered main store"
            );

            // re-inject poison pill and wait for crash of first instance
            skip.set(false);
            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                inputTopic,
                Collections.singletonList(
                    new KeyValue<>(1, 0)
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerSerializer.class,
                    IntegerSerializer.class,
                    new Properties()
                ),
                10L
            );
            waitForCondition(
                () -> streamInstanceOneRecovery.state() == KafkaStreams.State.ERROR,
                "Stream instance 1 did not go into error state"
            );
        }
    }

    private KafkaStreams buildWithDeduplicationTopology(final String stateDirPath) {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(storeName),
            Serdes.Integer(),
            Serdes.Integer())
        );
        builder.<Integer, Integer>stream(inputTopic)
            .transform(
                () -> new Transformer<Integer, Integer, KeyValue<Integer, Integer>>() {
                    private KeyValueStore<Integer, Integer> store;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void init(final ProcessorContext context) {
                        store = (KeyValueStore<Integer, Integer>) context.getStateStore(storeName);

                        final KeyValueIterator<Integer, Integer> it = store.all();
                        System.err.println("mjsax: store content begin");
                        while (it.hasNext()) {
                            final KeyValue<Integer, Integer> next = it.next();
                            System.err.println("mjsax: key/value -> " + next.key + "/" + next.value);
                        }
                        System.err.println("mjsax: store content end");
                    }

                    @Override
                    public KeyValue<Integer, Integer> transform(final Integer key, final Integer value) {
                        if (skip.get()) {
                            System.err.println("mjsax skip key " + key);
                            return null;
                        }

                        if (store.get(key) != null) {
                            System.err.println("mjsax found duplicate for key " + key);
                            return null;
                        }

                        System.err.println("mjsax put key " + key);
                        store.put(key, value);
                        store.flush();

                        if (key == 1) {
                            skip.set(true);
                            throw new RuntimeException("Injected test error");
                        }

                        return KeyValue.pair(key, value);
                    }

                    @Override
                    public void close() { }
                },
                storeName
            )
            .to(outputTopic);

        return new KafkaStreams(builder.build(), props(stateDirPath));
    }

    private Properties props(final String stateDirPath) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirPath);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return streamsConfiguration;
    }
}
