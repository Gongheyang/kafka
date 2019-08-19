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
package org.apache.kafka.connect.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A handle to a connector executing in a Connect cluster.
 */
public class ConnectorHandle {

    private static final Logger log = LoggerFactory.getLogger(ConnectorHandle.class);

    private final String connectorName;
    private final Map<String, TaskHandle> taskHandles = new ConcurrentHashMap<>();
    private final StartAndStopCounter startAndStopCounter = new StartAndStopCounter();
    private final RecordLatches recordLatches;

    public ConnectorHandle(String connectorName) {
        this.connectorName = connectorName;
        this.recordLatches = new RecordLatches("Connector " + connectorName);
    }

    /**
     * Get or create a task handle for a given task id. The task need not be created when this method is called. If the
     * handle is called before the task is created, the task will bind to the handle once it starts (or restarts).
     *
     * @param taskId the task id
     * @return a non-null {@link TaskHandle}
     */
    public TaskHandle taskHandle(String taskId) {
        return taskHandles.computeIfAbsent(taskId, k -> new TaskHandle(this, taskId));
    }

    /**
     * Get the connector's name corresponding to this handle.
     *
     * @return the connector's name
     */
    public String name() {
        return connectorName;
    }

    /**
     * Get the list of tasks handles monitored by this connector handle.
     *
     * @return the task handle list
     */
    public Collection<TaskHandle> tasks() {
        return taskHandles.values();
    }

    /**
     * Delete the task handle for this task id.
     *
     * @param taskId the task id.
     */
    public void deleteTask(String taskId) {
        log.info("Removing handle for {} task in connector {}", taskId, connectorName);
        taskHandles.remove(taskId);
    }

    /**
     * Set the number of expected records for this connector.
     *
     * @param expected number of records
     */
    public void expectedRecords(int expected) {
        recordLatches.expectedRecords(expected);
    }

    /**
     * Set the number of expected records for this task.
     *
     * @param topic    the name of the topic onto which the records are expected
     * @param expected number of records
     */
    public void expectedRecords(String topic, int expected) {
        recordLatches.expectedRecords(topic, expected);
    }

    /**
     * Set the number of expected commits performed by this connector.
     *
     * @param expected number of commits
     */
    public void expectedCommits(int expected) {
        recordLatches.expectedCommits(expected);
    }

    /**
     * Record a message arrival at the connector.
     */
    public void record() {
        recordLatches.record();
    }

    /**
     * Record arrival of a batch of messages at the connector.
     *
     * @param batchSize the number of messages
     */
    public void record(int batchSize) {
        recordLatches.record(batchSize);
    }

    /**
     * Record a message arrival at the task and the connector overall.
     *
     * @param topic the name of the topic
     */
    public void record(String topic) {
        recordLatches.record(topic);
    }

    /**
     * Record arrival of a batch of messages at the task and the connector overall.
     *
     * @param topic     the name of the topic
     * @param batchSize the number of messages
     */
    public void record(String topic, int batchSize) {
        recordLatches.record(topic, batchSize);
    }

    /**
     * Record a message commit from the connector.
     */
    public void commit() {
        recordLatches.commit();
    }

    /**
     * Record commit on a batch of messages from the connector.
     *
     * @param batchSize the number of messages
     */
    public void commit(int batchSize) {
        recordLatches.commit(batchSize);
    }

    /**
     * Wait for this connector to meet the expected number of records as defined by {@code
     * expectedRecords}.
     *
     * @param timeout max duration to wait for records
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(long timeout) throws InterruptedException {
        recordLatches.awaitRecords(timeout);
    }

    /**
     * Wait for this connector to meet the expected number of records as defined by {@code
     * expectedRecords}.
     *
     * @param topic   the name of the topic
     * @param timeout max duration to wait for records
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(String topic, long timeout) throws InterruptedException {
        recordLatches.awaitRecords(topic, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Wait for this connector to meet the expected number of commits as defined by {@code
     * expectedCommits}.
     *
     * @param  timeout duration to wait for commits
     * @throws InterruptedException if another threads interrupts this one while waiting for commits
     */
    public void awaitCommits(long timeout) throws InterruptedException {
        recordLatches.awaitCommits(timeout);
    }

    /**
     * Record that this connector has been started. This should be called by the connector under
     * test.
     *
     * @see #expectedStarts(int)
     */
    public void recordConnectorStart() {
        startAndStopCounter.recordStart();
    }

    /**
     * Record that this connector has been stopped. This should be called by the connector under
     * test.
     *
     * @see #expectedStarts(int)
     */
    public void recordConnectorStop() {
        startAndStopCounter.recordStop();
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the connector using this handle
     * and all tasks using {@link TaskHandle} have completed the expected number of
     * starts, starting the counts at the time this method is called.
     *
     * <p>A test can call this method, specifying the number of times the connector and tasks
     * will each be stopped and started from that point (typically {@code expectedStarts(1)}).
     * The test should then change the connector or otherwise cause the connector to restart one or
     * more times, and then can call {@link StartAndStopLatch#await(long, TimeUnit)} to wait up to a
     * specified duration for the connector and all tasks to be started at least the specified
     * number of times.
     *
     * <p>This method does not track the number of times the connector and tasks are stopped, and
     * only tracks the number of times the connector and tasks are <em>started</em>.
     *
     * @param expectedStarts the minimum number of starts that are expected once this method is
     *                       called
     * @return the latch that can be used to wait for the starts to complete; never null
     */
    public StartAndStopLatch expectedStarts(int expectedStarts) {
        return expectedStarts(expectedStarts, true);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the connector using this handle
     * and optionally all tasks using {@link TaskHandle} have completed the expected number of
     * starts, starting the counts at the time this method is called.
     *
     * <p>A test can call this method, specifying the number of times the connector and tasks
     * will each be stopped and started from that point (typically {@code expectedStarts(1)}).
     * The test should then change the connector or otherwise cause the connector to restart one or
     * more times, and then can call {@link StartAndStopLatch#await(long, TimeUnit)} to wait up to a
     * specified duration for the connector and all tasks to be started at least the specified
     * number of times.
     *
     * <p>This method does not track the number of times the connector and tasks are stopped, and
     * only tracks the number of times the connector and tasks are <em>started</em>.
     *
     * @param expectedStarts the minimum number of starts that are expected once this method is
     *                       called
     * @param includeTasks  true if the latch should also wait for the tasks to be stopped the
     *                      specified minimum number of times
     * @return the latch that can be used to wait for the starts to complete; never null
     */
    public StartAndStopLatch expectedStarts(int expectedStarts, boolean includeTasks) {
        List<StartAndStopLatch> taskLatches = null;
        if (includeTasks) {
            taskLatches = taskHandles.values().stream()
                                     .map(task -> task.expectedStarts(expectedStarts))
                                     .collect(Collectors.toList());
        }
        return startAndStopCounter.expectedStarts(expectedStarts, taskLatches);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the connector using this handle
     * and optionally all tasks using {@link TaskHandle} have completed the minimum number of
     * stops, starting the counts at the time this method is called.
     *
     * <p>A test can call this method, specifying the number of times the connector and tasks
     * will each be stopped from that point (typically {@code expectedStops(1)}).
     * The test should then change the connector or otherwise cause the connector to stop (or
     * restart) one or more times, and then can call
     * {@link StartAndStopLatch#await(long, TimeUnit)} to wait up to a specified duration for the
     * connector and all tasks to be started at least the specified number of times.
     *
     * <p>This method does not track the number of times the connector and tasks are stopped, and
     * only tracks the number of times the connector and tasks are <em>started</em>.
     *
     * @param expectedStops the minimum number of starts that are expected once this method is
     *                      called
     * @return the latch that can be used to wait for the starts to complete; never null
     */
    public StartAndStopLatch expectedStops(int expectedStops) {
        return expectedStops(expectedStops, true);
    }

    /**
     * Obtain a {@link StartAndStopLatch} that can be used to wait until the connector using this handle
     * and optionally all tasks using {@link TaskHandle} have completed the minimum number of
     * stops, starting the counts at the time this method is called.
     *
     * <p>A test can call this method, specifying the number of times the connector and tasks
     * will each be stopped from that point (typically {@code expectedStops(1)}).
     * The test should then change the connector or otherwise cause the connector to stop (or
     * restart) one or more times, and then can call
     * {@link StartAndStopLatch#await(long, TimeUnit)} to wait up to a specified duration for the
     * connector and all tasks to be started at least the specified number of times.
     *
     * <p>This method does not track the number of times the connector and tasks are stopped, and
     * only tracks the number of times the connector and tasks are <em>started</em>.
     *
     * @param expectedStops the minimum number of starts that are expected once this method is
     *                      called
     * @param includeTasks  true if the latch should also wait for the tasks to be stopped the
     *                      specified minimum number of times
     * @return the latch that can be used to wait for the starts to complete; never null
     */
    public StartAndStopLatch expectedStops(int expectedStops, boolean includeTasks) {
        List<StartAndStopLatch> taskLatches = null;
        if (includeTasks) {
            taskLatches = taskHandles.values().stream()
                                     .map(task -> task.expectedStops(expectedStops))
                                     .collect(Collectors.toList());
        }
        return startAndStopCounter.expectedStops(expectedStops, taskLatches);
    }

    @Override
    public String toString() {
        return "ConnectorHandle{" +
                "connectorName='" + connectorName + '\'' +
                '}';
    }
}
