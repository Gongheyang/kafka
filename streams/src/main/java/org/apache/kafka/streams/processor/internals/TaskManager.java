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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskIdFormatException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.streams.processor.internals.Task.State.CREATED;
import static org.apache.kafka.streams.processor.internals.Task.State.RESTORING;
import static org.apache.kafka.streams.processor.internals.Task.State.RUNNING;

public class TaskManager {
    // initialize the task list
    // activeTasks needs to be concurrent as it can be accessed
    // by QueryableState
    private final Logger log;
    private final ChangelogReader changelogReader;
    private final UUID processId;
    private final String logPrefix;
    private final StreamsMetricsImpl streamsMetrics;
    private final ActiveTaskCreator activeTaskCreator;
    private final StandbyTaskCreator standbyTaskCreator;
    private final InternalTopologyBuilder builder;
    private final Admin adminClient;
    private final StateDirectory stateDirectory;

    private final Map<TaskId, Task> tasks = new TreeMap<>();
    // materializing this relationship because the lookup is on the hot path
    private final Map<TopicPartition, Task> partitionToTask = new HashMap<>();

    private Consumer<byte[], byte[]> mainConsumer;

    private DeleteRecordsResult deleteRecordsResult;

    private boolean rebalanceInProgress = false;  // if we are in the middle of a rebalance, it is not safe to commit

    TaskManager(final ChangelogReader changelogReader,
                final UUID processId,
                final String logPrefix,
                final StreamsMetricsImpl streamsMetrics,
                final ActiveTaskCreator activeTaskCreator,
                final StandbyTaskCreator standbyTaskCreator,
                final InternalTopologyBuilder builder,
                final Admin adminClient,
                final StateDirectory stateDirectory) {
        this.changelogReader = changelogReader;
        this.processId = processId;
        this.logPrefix = logPrefix;
        this.streamsMetrics = streamsMetrics;
        this.activeTaskCreator = activeTaskCreator;
        this.standbyTaskCreator = standbyTaskCreator;
        this.builder = builder;
        this.adminClient = adminClient;
        this.stateDirectory = stateDirectory;

        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());
    }

    void setMainConsumer(final Consumer<byte[], byte[]> mainConsumer) {
        this.mainConsumer = mainConsumer;
    }

    public UUID processId() {
        return processId;
    }

    InternalTopologyBuilder builder() {
        return builder;
    }

    boolean isRebalanceInProgress() {
        return rebalanceInProgress;
    }

    void handleRebalanceStart(final Set<String> subscribedTopics) {
        builder.addSubscribedTopicsFromMetadata(subscribedTopics, logPrefix);

        rebalanceInProgress = true;
    }

    void handleRebalanceComplete() {
        // we should pause consumer only within the listener since
        // before then the assignment has not been updated yet.
        mainConsumer.pause(mainConsumer.assignment());

        rebalanceInProgress = false;
    }

    void handleCorruption(final Map<TaskId, Set<TopicPartition>> taskWithChangelogs) {
        final LinkedHashMap<TaskId, RuntimeException> producerCloseExceptions = new LinkedHashMap<>();

        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : taskWithChangelogs.entrySet()) {
            final TaskId taskId = entry.getKey();
            final Task task = tasks.get(taskId);

            // this call is idempotent so even if the task is only CREATED we can still call it
            changelogReader.remove(task.changelogPartitions());

            // mark corrupted partitions to not be checkpointed, and then close the task as dirty
            final Set<TopicPartition> corruptedPartitions = entry.getValue();
            task.markChangelogAsCorrupted(corruptedPartitions);

            try {
                task.closeClean();
            } catch (final RuntimeException e) {
                log.error("Failed to close task {} cleanly while handling corrupted tasks. Attempting to re-close it as dirty.", task.id());
                task.closeDirty();

                // We need to recreate the producer as it could potentially be in illegal state.
                if (task.isActive()) {
                    try {
                        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(task.id());
                        activeTaskCreator.createTaskProducer(taskId);
                    } catch (final RuntimeException producerException) {
                        final String uncleanMessage = String.format("Failed to close task %s producer cleanly. " +
                            "Attempting to close remaining task producers before re-throwing:", task.id());
                        log.error(uncleanMessage, producerException);
                        producerCloseExceptions.putIfAbsent(task.id(), producerException);
                    }
                }
            }

            task.revive();
        }

        if (!producerCloseExceptions.isEmpty()) {
            final Map.Entry<TaskId, RuntimeException> firstEntry =
                producerCloseExceptions.entrySet().iterator().next();

            if (firstEntry.getValue() instanceof KafkaException) {
                log.error("Hit Kafka exception while closing first task {} producer", firstEntry.getKey());
                throw firstEntry.getValue();
            } else {
                throw new RuntimeException(
                    "Unexpected failure to close " + producerCloseExceptions.size() +
                        " task(s) producers [" + producerCloseExceptions.keySet() + "]. " +
                        "First unexpected exception (for task " + firstEntry.getKey() + ") follows.", firstEntry.getValue()
                );
            }
        }
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     * @throws StreamsException fatal error while creating / initializing the task
     *
     * public for upgrade testing only
     */
    public void handleAssignment(final Map<TaskId, Set<TopicPartition>> activeTasks,
                                 final Map<TaskId, Set<TopicPartition>> standbyTasks) {
        log.info("Handle new assignment with:\n" +
                     "\tNew active tasks: {}\n" +
                     "\tNew standby tasks: {}\n" +
                     "\tExisting active tasks: {}\n" +
                     "\tExisting standby tasks: {}",
                 activeTasks.keySet(), standbyTasks.keySet(), activeTaskIds(), standbyTaskIds());

        final Map<TaskId, Set<TopicPartition>> activeTasksToCreate = new TreeMap<>(activeTasks);
        final Map<TaskId, Set<TopicPartition>> standbyTasksToCreate = new TreeMap<>(standbyTasks);

        // first rectify all existing tasks
        final LinkedHashMap<TaskId, RuntimeException> taskCloseExceptions = new LinkedHashMap<>();
        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task task = iterator.next();
            if (activeTasks.containsKey(task.id()) && task.isActive()) {
                task.resume();
                activeTasksToCreate.remove(task.id());
            } else if (standbyTasks.containsKey(task.id()) && !task.isActive()) {
                task.resume();
                standbyTasksToCreate.remove(task.id());
            } else /* we previously owned this task, and we don't have it anymore, or it has changed active/standby state */ {
                cleanupTask(task);

                try {
                    task.closeClean();
                } catch (final RuntimeException e) {
                    final String uncleanMessage = String.format("Failed to close task %s cleanly. Attempting to close remaining tasks before re-throwing:", task.id());
                    log.error(uncleanMessage, e);
                    taskCloseExceptions.put(task.id(), e);
                    // We've already recorded the exception (which is the point of clean).
                    // Now, we should go ahead and complete the close because a half-closed task is no good to anyone.
                    task.closeDirty();
                } finally {
                    if (task.isActive()) {
                        try {
                            activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(task.id());
                        } catch (final RuntimeException e) {
                            final String uncleanMessage = String.format("Failed to close task %s cleanly. Attempting to close remaining tasks before re-throwing:", task.id());
                            log.error(uncleanMessage, e);
                            taskCloseExceptions.putIfAbsent(task.id(), e);
                        }
                    }
                }

                iterator.remove();
            }
        }

        if (!taskCloseExceptions.isEmpty()) {
            for (final Map.Entry<TaskId, RuntimeException> entry : taskCloseExceptions.entrySet()) {
                if (!(entry.getValue() instanceof TaskMigratedException)) {
                    if (entry.getValue() instanceof KafkaException) {
                        log.error("Hit Kafka exception while closing for first task {}", entry.getKey());
                        throw entry.getValue();
                    } else {
                        throw new RuntimeException(
                            "Unexpected failure to close " + taskCloseExceptions.size() +
                                " task(s) [" + taskCloseExceptions.keySet() + "]. " +
                                "First unexpected exception (for task " + entry.getKey() + ") follows.", entry.getValue()
                        );
                    }
                }
            }

            final Map.Entry<TaskId, RuntimeException> first = taskCloseExceptions.entrySet().iterator().next();
            // If all exceptions are task-migrated, we would just throw the first one.
            throw first.getValue();
        }

        if (!activeTasksToCreate.isEmpty()) {
            for (final Task task : activeTaskCreator.createTasks(mainConsumer, activeTasksToCreate)) {
                addNewTask(task);
            }
        }

        if (!standbyTasksToCreate.isEmpty()) {
            for (final Task task : standbyTaskCreator.createTasks(standbyTasksToCreate)) {
                addNewTask(task);
            }
        }

        builder.addSubscribedTopicsFromAssignment(
            activeTasks.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
            logPrefix
        );

        changelogReader.transitToRestoreActive();
    }

    private void addNewTask(final Task task) {
        final Task previous = tasks.put(task.id(), task);
        if (previous != null) {
            throw new IllegalStateException("Attempted to create a task that we already owned: " + task.id());
        }

        for (final TopicPartition topicPartition : task.inputPartitions()) {
            partitionToTask.put(topicPartition, task);
        }
    }

    /**
     * Tries to initialize any new or still-uninitialized tasks, then checks if they can/have completed restoration.
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     * @return {@code true} if all tasks are fully restored
     */
    boolean tryToCompleteRestoration() {
        boolean allRunning = true;

        final List<Task> restoringTasks = new LinkedList<>();
        for (final Task task : tasks.values()) {
            if (task.state() == CREATED) {
                try {
                    task.initializeIfNeeded();
                } catch (final LockException | TimeoutException e) {
                    // it is possible that if there are multiple threads within the instance that one thread
                    // trying to grab the task from the other, while the other has not released the lock since
                    // it did not participate in the rebalance. In this case we can just retry in the next iteration
                    log.debug("Could not initialize {} due to {}; will retry", task.id(), e);
                    allRunning = false;
                }
            }

            if (task.state() == RESTORING) {
                restoringTasks.add(task);
            }
        }

        if (allRunning && !restoringTasks.isEmpty()) {
            final Set<TopicPartition> restored = changelogReader.completedChangelogs();
            for (final Task task : restoringTasks) {
                if (restored.containsAll(task.changelogPartitions())) {
                    try {
                        task.completeRestoration();
                    } catch (final TimeoutException e) {
                        log.debug("Could not complete restoration for {} due to {}; will retry", task.id(), e);

                        allRunning = false;
                    }
                } else {
                    // we found a restoring task that isn't done restoring, which is evidence that
                    // not all tasks are running
                    allRunning = false;
                }
            }
        }

        if (allRunning) {
            // we can call resume multiple times since it is idempotent.
            mainConsumer.resume(mainConsumer.assignment());
        }

        return allRunning;
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void handleRevocation(final Collection<TopicPartition> revokedPartitions) {
        final Set<TopicPartition> remainingPartitions = new HashSet<>(revokedPartitions);

        for (final Task task : tasks.values()) {
            if (remainingPartitions.containsAll(task.inputPartitions())) {
                task.suspend();
            }
            remainingPartitions.removeAll(task.inputPartitions());
        }

        if (!remainingPartitions.isEmpty()) {
            log.warn("The following partitions {} are missing from the task partitions. It could potentially " +
                         "due to race condition of consumer detecting the heartbeat failure, or the tasks " +
                         "have been cleaned up by the handleAssignment callback.", remainingPartitions);
        }
    }

    /**
     * Closes active tasks as zombies, as these partitions have been lost and are no longer owned.
     * NOTE this method assumes that when it is called, EVERY task/partition has been lost and must
     * be closed as a zombie.
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void handleLostAll() {
        log.debug("Closing lost active tasks as zombies.");

        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task task = iterator.next();
            final Set<TopicPartition> inputPartitions = task.inputPartitions();
            // Even though we've apparently dropped out of the group, we can continue safely to maintain our
            // standby tasks while we rejoin.
            if (task.isActive()) {
                cleanupTask(task);
                task.closeDirty();
                iterator.remove();
                try {
                    activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(task.id());
                } catch (final RuntimeException e) {
                    log.warn("Error closing task producer for " + task.id() + " while handling lostAll", e);
                }
            }

            for (final TopicPartition inputPartition : inputPartitions) {
                partitionToTask.remove(inputPartition);
            }
        }
    }

    /**
     * @return Map from task id to its total offset summed across all state stores
     */
    public Map<TaskId, Long> getTaskOffsetSums() {
        final Map<TaskId, Long> taskOffsetSums = new HashMap<>();

        for (final TaskId id : tasksOnLocalStorage()) {
            if (isRunning(id)) {
                taskOffsetSums.put(id, Task.LATEST_OFFSET);
            } else {
                taskOffsetSums.put(id, 0L);
            }
        }
        return taskOffsetSums;
    }

    /**
     * Returns ids of tasks whose states are kept on the local storage. This includes active, standby, and previously
     * assigned but not yet cleaned up tasks
     */
    private Set<TaskId> tasksOnLocalStorage() {
        // A client could contain some inactive tasks whose states are still kept on the local storage in the following scenarios:
        // 1) the client is actively maintaining standby tasks by maintaining their states from the change log.
        // 2) the client has just got some tasks migrated out of itself to other clients while these task states
        //    have not been cleaned up yet (this can happen in a rolling bounce upgrade, for example).

        final Set<TaskId> locallyStoredTasks = new HashSet<>();

        final File[] stateDirs = stateDirectory.listTaskDirectories();
        if (stateDirs != null) {
            for (final File dir : stateDirs) {
                try {
                    final TaskId id = TaskId.parse(dir.getName());
                    // if the checkpoint file exists, the state is valid.
                    if (new File(dir, StateManagerUtil.CHECKPOINT_FILE_NAME).exists()) {
                        locallyStoredTasks.add(id);
                    }
                } catch (final TaskIdFormatException e) {
                    // there may be some unknown files that sits in the same directory,
                    // we should ignore these files instead trying to delete them as well
                }
            }
        }

        return locallyStoredTasks;
    }

    private void cleanupTask(final Task task) {
        // 1. remove the changelog partitions from changelog reader;
        // 2. remove the input partitions from the materialized map;
        // 3. remove the task metrics from the metrics registry
        if (!task.changelogPartitions().isEmpty()) {
            changelogReader.remove(task.changelogPartitions());
        }

        for (final TopicPartition inputPartition : task.inputPartitions()) {
            partitionToTask.remove(inputPartition);
        }

        final String threadId = Thread.currentThread().getName();
        streamsMetrics.removeAllTaskLevelSensors(threadId, task.id().toString());
    }

    void shutdown(final boolean clean) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task task = iterator.next();
            cleanupTask(task);

            if (clean) {
                try {
                    task.closeClean();
                } catch (final TaskMigratedException e) {
                    // just ignore the exception as it doesn't matter during shutdown
                    task.closeDirty();
                } catch (final RuntimeException e) {
                    firstException.compareAndSet(null, e);
                    task.closeDirty();
                }
            } else {
                task.closeDirty();
            }
            if (task.isActive()) {
                try {
                    activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(task.id());
                } catch (final RuntimeException e) {
                    if (clean) {
                        firstException.compareAndSet(null, e);
                    } else {
                        log.warn("Ignoring an exception while closing task " + task.id() + " producer.", e);
                    }
                }
            }
            iterator.remove();
        }

        try {
            activeTaskCreator.closeThreadProducerIfNeeded();
        } catch (final RuntimeException e) {
            if (clean) {
                firstException.compareAndSet(null, e);
            } else {
                log.warn("Ignoring an exception while closing thread producer.", e);
            }
        }

        final RuntimeException fatalException = firstException.get();
        if (fatalException != null) {
            throw new RuntimeException("Unexpected exception while closing task", fatalException);
        }
    }

    Set<TaskId> activeTaskIds() {
        return activeTaskStream()
            .map(Task::id)
            .collect(Collectors.toSet());
    }

    Set<TaskId> standbyTaskIds() {
        return standbyTaskStream()
            .map(Task::id)
            .collect(Collectors.toSet());
    }

    Task taskForInputPartition(final TopicPartition partition) {
        return partitionToTask.get(partition);
    }

    Map<TaskId, Task> tasks() {
        // not bothering with an unmodifiable map, since the tasks themselves are mutable, but
        // if any outside code modifies the map or the tasks, it would be a severe transgression.
        return tasks;
    }

    Map<TaskId, Task> activeTaskMap() {
        return activeTaskStream().collect(Collectors.toMap(Task::id, t -> t));
    }

    List<Task> activeTaskIterable() {
        return activeTaskStream().collect(Collectors.toList());
    }

    private Stream<Task> activeTaskStream() {
        return tasks.values().stream().filter(Task::isActive);
    }

    Map<TaskId, Task> standbyTaskMap() {
        return standbyTaskStream().collect(Collectors.toMap(Task::id, t -> t));
    }

    private Stream<Task> standbyTaskStream() {
        return tasks.values().stream().filter(t -> !t.isActive());
    }

    private boolean isRunning(final TaskId id) {
        final Task task = tasks.get(id);
        return task != null && task.isActive() && task.state() == RUNNING;
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     * @return number of committed offsets, or -1 if we are in the middle of a rebalance and cannot commit
     */
    int commitAll() {
        if (rebalanceInProgress) {
            return -1;
        } else {
            int commits = 0;
            for (final Task task : tasks.values()) {
                if (task.commitNeeded()) {
                    task.commit();
                    commits++;
                }
            }
            return commits;
        }
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int maybeCommitActiveTasksPerUserRequested() {
        if (rebalanceInProgress) {
            return -1;
        } else {
            int commits = 0;
            for (final Task task : activeTaskIterable()) {
                if (task.commitRequested() && task.commitNeeded()) {
                    task.commit();
                    commits++;
                }
            }
            return commits;
        }
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int process(final long now) {
        int processed = 0;

        for (final Task task : activeTaskIterable()) {
            try {
                if (task.process(now)) {
                    processed++;
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to process stream task {} since it got migrated to another thread already. " +
                             "Will trigger a new rebalance and close all tasks as zombies together.", task.id());
                throw e;
            } catch (final RuntimeException e) {
                log.error("Failed to process stream task {} due to the following error:", task.id(), e);
                throw e;
            }
        }

        return processed;
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int punctuate() {
        int punctuated = 0;

        for (final Task task : activeTaskIterable()) {
            try {
                if (task.maybePunctuateStreamTime()) {
                    punctuated++;
                }
                if (task.maybePunctuateSystemTime()) {
                    punctuated++;
                }
            } catch (final TaskMigratedException e) {
                log.info("Failed to punctuate stream task {} since it got migrated to another thread already. " +
                             "Will trigger a new rebalance and close all tasks as zombies together.", task.id());
                throw e;
            } catch (final KafkaException e) {
                log.error("Failed to punctuate stream task {} due to the following error:", task.id(), e);
                throw e;
            }
        }
        return punctuated;
    }

    void maybePurgeCommittedRecords() {
        // we do not check any possible exceptions since none of them are fatal
        // that should cause the application to fail, and we will try delete with
        // newer offsets anyways.
        if (deleteRecordsResult == null || deleteRecordsResult.all().isDone()) {

            if (deleteRecordsResult != null && deleteRecordsResult.all().isCompletedExceptionally()) {
                log.debug("Previous delete-records request has failed: {}. Try sending the new request now",
                          deleteRecordsResult.lowWatermarks());
            }

            final Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            for (final Task task : activeTaskIterable()) {
                for (final Map.Entry<TopicPartition, Long> entry : task.purgableOffsets().entrySet()) {
                    recordsToDelete.put(entry.getKey(), RecordsToDelete.beforeOffset(entry.getValue()));
                }
            }
            if (!recordsToDelete.isEmpty()) {
                deleteRecordsResult = adminClient.deleteRecords(recordsToDelete);
                log.trace("Sent delete-records request: {}", recordsToDelete);
            }
        }
    }

    /**
     * Produces a string representation containing useful information about the TaskManager.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the TaskManager instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    public String toString(final String indent) {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("TaskManager\n");
        stringBuilder.append(indent).append("\tMetadataState:\n");
        stringBuilder.append(indent).append("\tTasks:\n");
        for (final Task task : tasks.values()) {
            stringBuilder.append(indent)
                         .append("\t\t")
                         .append(task.id())
                         .append(" ")
                         .append(task.state())
                         .append(" ")
                         .append(task.getClass().getSimpleName())
                         .append('(').append(task.isActive() ? "active" : "standby").append(')');
        }
        return stringBuilder.toString();
    }

    Map<MetricName, Metric> producerMetrics() {
        return activeTaskCreator.producerMetrics();
    }

    Set<String> producerClientIds() {
        return activeTaskCreator.producerClientIds();
    }
}
