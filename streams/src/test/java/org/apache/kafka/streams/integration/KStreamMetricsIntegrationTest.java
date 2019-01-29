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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
public class KStreamMetricsIntegrationTest {
    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER =
        new EmbeddedKafkaCluster(NUM_BROKERS);
    public static final String PROCESS_LATENCY_AVG = "process-latency-avg";
    public static final String STREAM_PROCESSOR_NODE_METRICS = "stream-processor-node-metrics";
    public static final String PROCESS_LATENCY_MAX = "process-latency-max";
    public static final String PUNCTUATE_LATENCY_AVG = "punctuate-latency-avg";
    public static final String PUNCTUATE_LATENCY_MAX = "punctuate-latency-max";
    public static final String CREATE_LATENCY_AVG = "create-latency-avg";
    public static final String CREATE_LATENCY_MAX = "create-latency-max";
    public static final String DESTROY_LATENCY_AVG = "destroy-latency-avg";
    public static final String DESTROY_LATENCY_MAX = "destroy-latency-max";
    public static final String PROCESS_RATE = "process-rate";
    public static final String PROCESS_TOTAL = "process-total";
    public static final String PUNCTUATE_RATE = "punctuate-rate";
    public static final String PUNCTUATE_TOTAL = "punctuate-total";
    public static final String CREATE_RATE = "create-rate";
    public static final String CREATE_TOTAL = "create-total";
    public static final String DESTROY_RATE = "destroy-rate";
    public static final String DESTROY_TOTAL = "destroy-total";
    public static final String FORWARD_TOTAL = "forward-total";
    public static final String STREAM_STRING = "stream";

    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String streamInput;
    private String metricOutput;
    private KStream<Integer, String> stream;

    final String appId = "stream-metrics-test";

    @Before
    public void before() throws InterruptedException {
        builder = new StreamsBuilder();
        createTopics();
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.name);
    }


    @Test
    public void testStreamMetric() throws Exception {
        startApplication();

        Thread.sleep(10000);

        testProcessorMetric();

        closeApplication();

        Thread.sleep(10000);

        final List<Metric> listMetricAfterClosingApp = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream().filter(m -> m.metricName().group().contains(STREAM_STRING)).collect(Collectors.toList());
        Assert.assertEquals(0, listMetricAfterClosingApp.size());

    }

    private void testProcessorMetric() {
        final List<Metric> listMetricProcessor = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream().filter(m -> m.metricName().group().equals(STREAM_PROCESSOR_NODE_METRICS)).collect(Collectors.toList());
        testMetricByName(listMetricProcessor, PROCESS_LATENCY_AVG, 3);
        testMetricByName(listMetricProcessor, PROCESS_LATENCY_MAX, 3);
        testMetricByName(listMetricProcessor, PUNCTUATE_LATENCY_AVG, 3);
        testMetricByName(listMetricProcessor, PUNCTUATE_LATENCY_MAX, 3);
        testMetricByName(listMetricProcessor, CREATE_LATENCY_AVG, 3);
        testMetricByName(listMetricProcessor, CREATE_LATENCY_MAX, 3);
        testMetricByName(listMetricProcessor, DESTROY_LATENCY_AVG, 3);
        testMetricByName(listMetricProcessor, DESTROY_LATENCY_MAX, 3);
        testMetricByName(listMetricProcessor, PROCESS_RATE, 3);
        testMetricByName(listMetricProcessor, PROCESS_TOTAL, 3);
        testMetricByName(listMetricProcessor, PUNCTUATE_RATE, 3);
        testMetricByName(listMetricProcessor, PUNCTUATE_TOTAL, 3);
        testMetricByName(listMetricProcessor, CREATE_RATE, 3);
        testMetricByName(listMetricProcessor, CREATE_TOTAL, 3);
        testMetricByName(listMetricProcessor, DESTROY_RATE, 3);
        testMetricByName(listMetricProcessor, DESTROY_TOTAL, 3);
        testMetricByName(listMetricProcessor, FORWARD_TOTAL, 3);
    }

    private void testMetricByName(List<Metric> listMetric, String metricName, int numMetric) {
        List<Metric> metrics = listMetric.stream().filter(m -> m.metricName().name().equals(metricName)).collect(Collectors.toList());
        Assert.assertEquals(numMetric, metrics.size());
        for (Metric m : metrics) {
            Assert.assertNotNull(m.metricValue());
        }
    }

    private void createTopics() throws InterruptedException {
        streamInput = "streamInput";
        metricOutput = "metricOutput";
        CLUSTER.createTopic(streamInput, 1, 1);
        CLUSTER.createTopics(metricOutput);
    }

    private void startApplication() {
        stream = builder.stream(streamInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.to(metricOutput, Produced.with(Serdes.Integer(), Serdes.String()));
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }

    private void closeApplication() throws IOException {
        kafkaStreams.close();
        kafkaStreams.cleanUp();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

}
