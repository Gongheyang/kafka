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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.LogReporter;
import org.apache.kafka.connect.runtime.errors.OperationExecutor;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceExecutor;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest(WorkerSinkTask.class)
@PowerMockIgnore("javax.management.*")
public class WorkerTaskWithErrorHandlingTest {

    private static final String TOPIC = "test";
    private static final int PARTITION1 = 12;
    private static final int PARTITION2 = 13;
    private static final int PARTITION3 = 14;
    private static final long FIRST_OFFSET = 45;
    private static final Schema KEY_SCHEMA = Schema.INT32_SCHEMA;
    private static final int KEY = 12;
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final String VALUE = "VALUE";
    private static final byte[] RAW_KEY = "key".getBytes();
    private static final byte[] RAW_VALUE = "value".getBytes();

    private static final Map<String, String> TASK_PROPS = new HashMap<>();

    static {
        TASK_PROPS.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        TASK_PROPS.put(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());
    }

    private static final TaskConfig TASK_CONFIG = new TaskConfig(TASK_PROPS);

    private static final Map<String, String> OPERATION_EXECUTOR_PROPS = new HashMap<>();

    static {
        OPERATION_EXECUTOR_PROPS.put(RetryWithToleranceExecutor.TOLERANCE_LIMIT, "all");
        // wait up to 1 minute for an operation
        OPERATION_EXECUTOR_PROPS.put(RetryWithToleranceExecutor.RETRY_TIMEOUT, "60000");
        // wait up 5 seconds between subsequent retries
        OPERATION_EXECUTOR_PROPS.put(RetryWithToleranceExecutor.RETRY_DELAY_MAX_MS, "5000");
    }

    private ConnectorTaskId taskId = new ConnectorTaskId("job", 0);
    private TargetState initialState = TargetState.STARTED;
    private Time time;
    private MockConnectMetrics metrics;
    @Mock
    private SinkTask sinkTask;
    private Capture<WorkerSinkTaskContext> sinkTaskContext = EasyMock.newCapture();
    private WorkerConfig workerConfig;
    @Mock
    private PluginClassLoader pluginLoader;
    @Mock
    private Converter keyConverter;
    @Mock
    private Converter valueConverter;
    @Mock
    private HeaderConverter headerConverter;
    private WorkerSinkTask workerTask;
    @Mock
    private KafkaConsumer<byte[], byte[]> consumer;
    private Capture<ConsumerRebalanceListener> rebalanceListener = EasyMock.newCapture();
    @Mock
    private TaskStatus.Listener statusListener;

    private ErrorHandlingMetrics errorHandlingMetrics;

    private long recordsReturned;

    @Before
    public void setup() {
        time = new MockTime(0, 0, 0);
        metrics = new MockConnectMetrics();
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter.schemas.enable", "false");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        pluginLoader = PowerMock.createMock(PluginClassLoader.class);
        workerConfig = new StandaloneConfig(workerProps);
        recordsReturned = 0;
        errorHandlingMetrics = new ErrorHandlingMetrics(taskId, metrics);
    }

    @Test
    public void testErrorHandlingInSinkTasks() throws Exception {
        LogReporter reporter = new LogReporter();
        Map<String, Object> reportProps = new HashMap<>();
        reportProps.put(LogReporter.LOG_ENABLE, "true");
        reportProps.put(LogReporter.LOG_INCLUDE_MESSAGES, "true");
        reporter.configure(reportProps);
        reporter.setMetrics(errorHandlingMetrics);

        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor(time);
        executor.configure(OPERATION_EXECUTOR_PROPS);
        executor.setMetrics(errorHandlingMetrics);
        createTask(initialState, new ProcessingContext(Collections.singletonList(reporter)), executor);

        expectInitializeTask();

        // valid json
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>(TOPIC, PARTITION1, FIRST_OFFSET, null, "{\"a\": 10}".getBytes());
        // bad json
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>(TOPIC, PARTITION2, FIRST_OFFSET, null, "{\"a\" 10}".getBytes());

        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andReturn(records(record1));
        EasyMock.expect(consumer.poll(EasyMock.anyLong())).andReturn(records(record2));

        sinkTask.put(EasyMock.anyObject());
        EasyMock.expectLastCall().times(2);

        PowerMock.replayAll();

        workerTask.initialize(TASK_CONFIG);
        workerTask.initializeAndStart();
        workerTask.iteration();

        workerTask.iteration();

        // two records were consumed from Kafka
        assertSinkMetricValue("sink-record-read-total", 2.0);
        // only one was written to the task
        assertSinkMetricValue("sink-record-send-total", 1.0);
        // one record completely failed (converter issues)
        assertErrorHandlingMetricValue("processing-errors", 1.0);
        // 2 failures in the transformation, and 1 in the converter
        assertErrorHandlingMetricValue("processing-failures", 3.0);
        // one record completely failed (converter issues), and thus was skipped
        assertErrorHandlingMetricValue("record-skipped", 1.0);

        PowerMock.verifyAll();
    }

    private void assertSinkMetricValue(String name, double expected) {
        ConnectMetrics.MetricGroup sinkTaskGroup = workerTask.sinkTaskMetricsGroup().metricGroup();
        double measured = metrics.currentMetricValueAsDouble(sinkTaskGroup, name);
        assertEquals(expected, measured, 0.001d);
    }

    private void assertErrorHandlingMetricValue(String name, double expected) {
        ConnectMetrics.MetricGroup sinkTaskGroup = errorHandlingMetrics.metricGroup();
        double measured = metrics.currentMetricValueAsDouble(sinkTaskGroup, name);
        assertEquals(expected, measured, 0.001d);
    }

    private void expectInitializeTask() throws Exception {
        PowerMock.expectPrivate(workerTask, "createConsumer").andReturn(consumer);
        consumer.subscribe(EasyMock.eq(Collections.singletonList(TOPIC)), EasyMock.capture(rebalanceListener));
        PowerMock.expectLastCall();

        sinkTask.initialize(EasyMock.capture(sinkTaskContext));
        PowerMock.expectLastCall();
        sinkTask.start(TASK_PROPS);
        PowerMock.expectLastCall();
    }

    private void createTask(TargetState initialState, ProcessingContext context, OperationExecutor executor) {
        JsonConverter converter = new JsonConverter();
        Map<String, Object> oo = workerConfig.originalsWithPrefix("value.converter.");
        oo.put("converter.type", "value");
        oo.put("schemas.enable", "false");
        converter.configure(oo);

        TransformationChain<SinkRecord> sinkTransforms = new TransformationChain<>(Collections.singletonList(new FaultyPassthrough<SinkRecord>()));

        workerTask = PowerMock.createPartialMock(
                WorkerSinkTask.class, new String[]{"createConsumer"},
                taskId, sinkTask, statusListener, initialState, workerConfig, metrics, converter, converter,
                headerConverter, sinkTransforms, pluginLoader, time, context, executor);
    }

    private ConsumerRecords<byte[], byte[]> records(ConsumerRecord<byte[], byte[]> record) {
        return new ConsumerRecords<>(Collections.singletonMap(
                new TopicPartition(record.topic(), record.partition()), Collections.singletonList(record)));
    }

    private abstract static class TestSinkTask extends SinkTask {
    }

    static class FaultyPassthrough<R extends ConnectRecord<R>> implements Transformation<R> {

        private static final Logger log = LoggerFactory.getLogger(FaultyPassthrough.class);

        private static final String MOD_CONFIG = "mod";
        private static final int MOD_CONFIG_DEFAULT = 3;

        public static final ConfigDef CONFIG_DEF = new ConfigDef()
                .define(MOD_CONFIG, ConfigDef.Type.INT, MOD_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, "Pass records without failure only if timestamp % mod == 0");

        private int mod = MOD_CONFIG_DEFAULT;

        private int invocations = 0;

        @Override
        public R apply(R record) {
            invocations++;
            if (invocations % mod == 0) {
                log.debug("Succeeding record: {} where invocations={}", record, invocations);
                return record;
            } else {
                log.debug("Failing record: {} at invocations={}", record, invocations);
                throw new RetriableException("Bad invocations " + invocations + " for mod " + mod);
            }
        }

        @Override
        public ConfigDef config() {
            return CONFIG_DEF;
        }

        @Override
        public void close() {
            log.info("Shutting down transform");
        }

        @Override
        public void configure(Map<String, ?> configs) {
            final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
            mod = Math.max(config.getInt(MOD_CONFIG), 2);
            log.info("Configuring {}. Setting mod to {}", this.getClass(), mod);
        }
    }
}
