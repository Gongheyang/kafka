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
package org.apache.kafka.streams.kstream.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_LEVEL_GROUP;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest({StreamsMetricsImpl.class, Sensor.class, ThreadMetrics.class})
public class TaskMetricsTest {

    private final static String THREAD_ID = "test-thread";
    private final static String TASK_ID = "test-task";

    private final StreamsMetricsImpl streamsMetrics = createMock(StreamsMetricsImpl.class);
    private final Sensor expectedSensor = createMock(Sensor.class);
    private final Map<String, String> tagMap = Collections.singletonMap("hello", "world");

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {Version.LATEST},
            {Version.FROM_100_TO_24}
        });
    }

    @Parameter
    public Version builtInMetricsVersion;

    @Before
    public void setUp() {
        expect(streamsMetrics.version()).andReturn(builtInMetricsVersion).anyTimes();
        mockStatic(StreamsMetricsImpl.class);
    }

    @Test
    public void shouldGetProcessLatencySensor() {
        final String operation = "process-latency";
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.DEBUG))
            .andReturn(expectedSensor);
        if (builtInMetricsVersion == Version.LATEST) {
            final String avgLatencyDescription = "The average processing latency";
            final String maxLatencyDescription = "The maximum processing latency";
            expect(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).andReturn(tagMap);
            StreamsMetricsImpl.addAvgAndMaxToSensor(
                expectedSensor,
                TASK_LEVEL_GROUP,
                tagMap,
                operation,
                avgLatencyDescription,
                maxLatencyDescription
            );
        }
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.processLatencySensor(THREAD_ID, TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetPunctuateSensor() {
        final String operation = "punctuate";
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.DEBUG))
            .andReturn(expectedSensor);
        if (builtInMetricsVersion == Version.LATEST) {
            final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
            final String totalDescription = "The total number of punctuate calls";
            final String rateDescription = "The average per-second number of punctuate calls";
            final String avgLatencyDescription = "The average punctuate latency";
            final String maxLatencyDescription = "The maximum punctuate latency";
            expect(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).andReturn(tagMap);
            StreamsMetricsImpl.addInvocationRateAndCountToSensor(
                expectedSensor,
                TASK_LEVEL_GROUP,
                tagMap,
                operation,
                rateDescription,
                totalDescription
            );
            StreamsMetricsImpl.addAvgAndMaxToSensor(
                expectedSensor,
                TASK_LEVEL_GROUP,
                tagMap,
                operationLatency,
                avgLatencyDescription,
                maxLatencyDescription
            );
        }
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.punctuateSensor(THREAD_ID, TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetCommitSensor() {
        final String operation = "commit";
        final String operationLatency = operation + StreamsMetricsImpl.LATENCY_SUFFIX;
        final String totalDescription = "The total number of commit calls";
        final String rateDescription = "The average per-second number of commit calls";
        final String avgLatencyDescription = "The average commit latency";
        final String maxLatencyDescription = "The maximum commit latency";
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.DEBUG)).andReturn(expectedSensor);
        expect(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operationLatency,
            avgLatencyDescription,
            maxLatencyDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.commitSensor(THREAD_ID, TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetEnforcedProcessingSensor() {
        final String operation = "enforced-processing";
        final String totalDescription = "The total number of occurrences of enforced-processing operations";
        final String rateDescription = "The average number of occurrences of enforced-processing operations per second";
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.DEBUG)).andReturn(expectedSensor);
        expect(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.enforcedProcessingSensor(THREAD_ID, TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetRecordLatenessSensor() {
        final String operation = "record-lateness";
        final String avgDescription = "The observed average lateness of records";
        final String maxDescription = "The observed maximum lateness of records";
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.DEBUG)).andReturn(expectedSensor);
        expect(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addAvgAndMaxToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            avgDescription,
            maxDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.recordLatenessSensor(THREAD_ID, TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetDroppedRecordsSensor() {
        final String operation = "dropped-records";
        final String totalDescription = "The total number of dropped records";
        final String rateDescription = "The average per-second number of dropped records";
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, operation, RecordingLevel.INFO)).andReturn(expectedSensor);
        expect(streamsMetrics.taskLevelTagMap(THREAD_ID, TASK_ID)).andReturn(tagMap);
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            expectedSensor,
            TASK_LEVEL_GROUP,
            tagMap,
            operation,
            rateDescription,
            totalDescription
        );
        replay(StreamsMetricsImpl.class, streamsMetrics);

        final Sensor sensor = TaskMetrics.droppedRecordsSensor(THREAD_ID, TASK_ID, streamsMetrics);

        verify(StreamsMetricsImpl.class, streamsMetrics);
        assertThat(sensor, is(expectedSensor));
    }

    @Test
    public void shouldGetDroppedOrSkippedRecordsSensor() {
        mockStatic(ThreadMetrics.class);
        if (builtInMetricsVersion == Version.FROM_100_TO_24) {
            expect(ThreadMetrics.skipRecordSensor(THREAD_ID, streamsMetrics)).andReturn(expectedSensor);
            replay(ThreadMetrics.class, StreamsMetricsImpl.class, streamsMetrics);

            final Sensor sensor = TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor(THREAD_ID, TASK_ID, streamsMetrics);

            verify(ThreadMetrics.class);
            assertThat(sensor, is(expectedSensor));
        } else {
            shouldGetDroppedRecordsSensor();
        }
    }
}