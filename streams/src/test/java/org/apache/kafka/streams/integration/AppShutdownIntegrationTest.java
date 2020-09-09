package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.ShutdownRequestedException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.*;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;


@Category(IntegrationTest.class)
public class AppShutdownIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @Rule
    public TestName testName = new TestName();


    @Test
    public void shouldSendShutDownSignal() throws Exception {
        //
        //
        // Also note that this is an integration test because so many components have to come together to
        // ensure these configurations wind up where they belong, and any number of future code changes
        // could break this change.

        final String testId = safeUniqueTestName(getClass(), testName);
        final String appId = "appId_" + testId;
        final String inputTopic = "input" + testId;

        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, inputTopic);


        final StreamsBuilder builder = new StreamsBuilder();


        final List<KeyValue<Object, Object>> processorValueCollector = new ArrayList<>();

        builder.stream(inputTopic).process(() -> new ShutdownProcessor(processorValueCollector), Named.as("process"));

        final Properties properties = mkObjectProperties(
                mkMap(
                        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                        mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                        mkEntry(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "5"),
                        mkEntry(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, "6"),
                        mkEntry(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, "7"),
                        mkEntry(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, "480000")
                )
        );


        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            final CountDownLatch latch = new CountDownLatch(1);

            kafkaStreams.start();
            produceMessages(0L, inputTopic);

            latch.await(10, TimeUnit.SECONDS);

            assertThat(processorValueCollector.size(), equalTo(5));
        }
    }

    private void produceMessages(final long timestamp, final String streamOneInput) throws ShutdownRequestedException {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                streamOneInput,
                Arrays.asList(
                        new KeyValue<>(1, "A"),
                        new KeyValue<>(2, "B"),
                        new KeyValue<>(3, "C"),
                        new KeyValue<>(4, "D"),
                        new KeyValue<>(5, "E")),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        IntegerSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                timestamp);
    }
}


class ShutdownProcessor extends AbstractProcessor<Object, Object> {
    final List<KeyValue<Object, Object>> valueList;

    ShutdownProcessor(final List<KeyValue<Object, Object>> valueList) {
        this.valueList = valueList;
    }

    @Override
    public void init(final ProcessorContext context) {
//        throw new ShutdownRequestedException("integration test");
    }

    @Override
    public void process(final Object key, final Object value) {
        valueList.add(new KeyValue<>(key, value));
        throw new ShutdownRequestedException("integration test");
    }


    @Override
    public void close() {

    }
}
