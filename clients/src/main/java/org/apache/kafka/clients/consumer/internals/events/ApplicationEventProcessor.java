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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.Acknowledgements;
import org.apache.kafka.clients.consumer.internals.CachedSupplier;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.OffsetAndTimestampInternal;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An {@link EventProcessor} that is created and executes in the {@link ConsumerNetworkThread network thread}
 * which processes {@link ApplicationEvent application events} generated by the application thread.
 */
public class ApplicationEventProcessor implements EventProcessor<ApplicationEvent> {

    private final Logger log;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final RequestManagers requestManagers;

    public ApplicationEventProcessor(final LogContext logContext,
                                     final RequestManagers requestManagers,
                                     final ConsumerMetadata metadata,
                                     final SubscriptionState subscriptions) {
        this.log = logContext.logger(ApplicationEventProcessor.class);
        this.requestManagers = requestManagers;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
    }

    @SuppressWarnings({"CyclomaticComplexity"})
    @Override
    public void process(ApplicationEvent event) {
        switch (event.type()) {
            case COMMIT_ASYNC:
                process((AsyncCommitEvent) event);
                return;

            case COMMIT_SYNC:
                process((SyncCommitEvent) event);
                return;

            case POLL:
                process((PollEvent) event);
                return;

            case FETCH_COMMITTED_OFFSETS:
                process((FetchCommittedOffsetsEvent) event);
                return;

            case ASSIGNMENT_CHANGE:
                process((AssignmentChangeEvent) event);
                return;

            case TOPIC_METADATA:
                process((TopicMetadataEvent) event);
                return;

            case ALL_TOPICS_METADATA:
                process((AllTopicsMetadataEvent) event);
                return;

            case LIST_OFFSETS:
                process((ListOffsetsEvent) event);
                return;

            case CHECK_AND_UPDATE_POSITIONS:
                process((CheckAndUpdatePositionsEvent) event);
                return;

            case SUBSCRIPTION_CHANGE:
                process((SubscriptionChangeEvent) event);
                return;

            case UNSUBSCRIBE:
                process((UnsubscribeEvent) event);
                return;

            case CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED:
                process((ConsumerRebalanceListenerCallbackCompletedEvent) event);
                return;

            case COMMIT_ON_CLOSE:
                process((CommitOnCloseEvent) event);
                return;

            case SHARE_FETCH:
                process((ShareFetchEvent) event);
                return;

            case SHARE_ACKNOWLEDGE_SYNC:
                process((ShareAcknowledgeSyncEvent) event);
                return;

            case SHARE_ACKNOWLEDGE_ASYNC:
                process((ShareAcknowledgeAsyncEvent) event);
                return;

            case SHARE_SUBSCRIPTION_CHANGE:
                process((ShareSubscriptionChangeEvent) event);
                return;

            case SHARE_UNSUBSCRIBE:
                process((ShareUnsubscribeEvent) event);
                return;

            case SHARE_ACKNOWLEDGE_ON_CLOSE:
                process((ShareAcknowledgeOnCloseEvent) event);
                return;

            default:
                log.warn("Application event type {} was not expected", event.type());
        }
    }

    private void process(final PollEvent event) {
        if (requestManagers.commitRequestManager.isPresent()) {
            requestManagers.commitRequestManager.ifPresent(m -> m.updateAutoCommitTimer(event.pollTimeMs()));
            requestManagers.consumerHeartbeatRequestManager.ifPresent(hrm -> hrm.resetPollTimer(event.pollTimeMs()));
        } else {
            requestManagers.shareHeartbeatRequestManager.ifPresent(hrm -> hrm.resetPollTimer(event.pollTimeMs()));
        }
    }

    private void process(final AsyncCommitEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        CompletableFuture<Void> future = manager.commitAsync(event.offsets());
        future.whenComplete(complete(event.future()));
    }

    private void process(final SyncCommitEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        CompletableFuture<Void> future = manager.commitSync(event.offsets(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private void process(final FetchCommittedOffsetsEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            event.future().completeExceptionally(new KafkaException("Unable to fetch committed " +
                    "offset because the CommittedRequestManager is not available. Check if group.id was set correctly"));
            return;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = manager.fetchOffsets(event.partitions(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    /**
     * Commit all consumed if auto-commit is enabled. Note this will trigger an async commit,
     * that will not be retried if the commit request fails.
     */
    private void process(final AssignmentChangeEvent event) {
        if (requestManagers.commitRequestManager.isPresent()) {
            CommitRequestManager manager = requestManagers.commitRequestManager.get();
            manager.updateAutoCommitTimer(event.currentTimeMs());
            manager.maybeAutoCommitAsync();
        }

        log.info("Assigned to partition(s): {}", event.partitions().stream().map(TopicPartition::toString).collect(Collectors.joining(", ")));
        try {
            if (subscriptions.assignFromUser(new HashSet<>(event.partitions())))
                metadata.requestUpdateForNewTopics();

            event.future().complete(null);
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    /**
     * Handles ListOffsetsEvent by fetching the offsets for the given partitions and timestamps.
     */
    private void process(final ListOffsetsEvent event) {
        final CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> future =
            requestManagers.offsetsRequestManager.fetchOffsets(event.timestampsToSearch(), event.requireTimestamps());
        future.whenComplete(complete(event.future()));
    }

    /**
     * Process event that indicates that the subscription changed. This will make the
     * consumer join the group if it is not part of it yet, or send the updated subscription if
     * it is already a member.
     */
    private void process(final SubscriptionChangeEvent ignored) {
        if (!requestManagers.consumerHeartbeatRequestManager.isPresent()) {
            log.warn("Group membership manager not present when processing a subscribe event");
            return;
        }
        requestManagers.consumerHeartbeatRequestManager.get().membershipManager().onSubscriptionUpdated();
    }

    /**
     * Process event indicating that the consumer unsubscribed from all topics. This will make
     * the consumer release its assignment and send a request to leave the group.
     *
     * @param event Unsubscribe event containing a future that will complete when the callback
     *              execution for releasing the assignment completes, and the request to leave
     *              the group is sent out.
     */
    private void process(final UnsubscribeEvent event) {
        if (requestManagers.consumerHeartbeatRequestManager.isPresent()) {
            CompletableFuture<Void> future = requestManagers.consumerHeartbeatRequestManager.get().membershipManager().leaveGroup();
            future.whenComplete(complete(event.future()));
        } else {
            // If the consumer is not using the group management capabilities, we still need to clear all assignments it may have.
            subscriptions.unsubscribe();
            event.future().complete(null);
        }
    }

    /**
     * Check if all assigned partitions have fetch positions. If there are missing positions, fetch offsets and use
     * them to update positions in the subscription state.
     */
    private void process(final CheckAndUpdatePositionsEvent event) {
        CompletableFuture<Boolean> future = requestManagers.offsetsRequestManager.updateFetchPositions(event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private void process(final TopicMetadataEvent event) {
        final CompletableFuture<Map<String, List<PartitionInfo>>> future =
                requestManagers.topicMetadataRequestManager.requestTopicMetadata(event.topic(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private void process(final AllTopicsMetadataEvent event) {
        final CompletableFuture<Map<String, List<PartitionInfo>>> future =
                requestManagers.topicMetadataRequestManager.requestAllTopicsMetadata(event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private void process(final ConsumerRebalanceListenerCallbackCompletedEvent event) {
        if (!requestManagers.consumerHeartbeatRequestManager.isPresent()) {
            log.warn(
                "An internal error occurred; the group membership manager was not present, so the notification of the {} callback execution could not be sent",
                event.methodName()
            );
            return;
        }
        requestManagers.consumerHeartbeatRequestManager.get().membershipManager().consumerRebalanceListenerCallbackCompleted(event);
    }

    private void process(@SuppressWarnings("unused") final CommitOnCloseEvent event) {
        if (!requestManagers.commitRequestManager.isPresent())
            return;
        log.debug("Signal CommitRequestManager closing");
        requestManagers.commitRequestManager.get().signalClose();
    }

    /**
     * Process event that tells the share consume request manager to fetch more records.
     */
    private void process(final ShareFetchEvent event) {
        requestManagers.shareConsumeRequestManager.ifPresent(scrm -> scrm.fetch(event.acknowledgementsMap()));
    }

    /**
     * Process event that indicates the consumer acknowledged delivery of records synchronously.
     */
    private void process(final ShareAcknowledgeSyncEvent event) {
        if (!requestManagers.shareConsumeRequestManager.isPresent()) {
            return;
        }

        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future =
                manager.commitSync(event.acknowledgementsMap(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    /**
     * Process event that indicates the consumer acknowledged delivery of records asynchronously.
     */
    private void process(final ShareAcknowledgeAsyncEvent event) {
        if (!requestManagers.shareConsumeRequestManager.isPresent()) {
            return;
        }

        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        manager.commitAsync(event.acknowledgementsMap());
    }

    /**
     * Process event that indicates that the subscription changed for a share group. This will make the
     * consumer join the share group if it is not part of it yet, or send the updated subscription if
     * it is already a member.
     */
    private void process(final ShareSubscriptionChangeEvent ignored) {
        if (!requestManagers.shareHeartbeatRequestManager.isPresent()) {
            log.warn("Group membership manager not present when processing a subscribe event");
            return;
        }
        requestManagers.shareHeartbeatRequestManager.get().membershipManager().onSubscriptionUpdated();
    }

    /**
     * Process event indicating that the consumer unsubscribed from all topics. This will make
     * the consumer release its assignment and send a request to leave the share group.
     *
     * @param event Unsubscribe event containing a future that will complete when the callback
     *              execution for releasing the assignment completes, and the request to leave
     *              the group is sent out.
     */
    private void process(final ShareUnsubscribeEvent event) {
        if (!requestManagers.shareHeartbeatRequestManager.isPresent()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing an unsubscribe event");
            event.future().completeExceptionally(error);
            return;
        }
        CompletableFuture<Void> future = requestManagers.shareHeartbeatRequestManager.get().membershipManager().leaveGroup();
        // The future will be completed on heartbeat sent
        future.whenComplete(complete(event.future()));
    }

    /**
     * Process event indicating that the consumer is closing. This will make the consumer
     * complete pending acknowledgements.
     *
     * @param event Acknowledge-on-close event containing a future that will complete when
     *              the acknowledgements have responses.
     */
    private void process(final ShareAcknowledgeOnCloseEvent event) {
        if (!requestManagers.shareConsumeRequestManager.isPresent()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing an acknowledge-on-close event");
            event.future().completeExceptionally(error);
            return;
        }

        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        CompletableFuture<Void> future = manager.acknowledgeOnClose(event.acknowledgementsMap(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private <T> BiConsumer<? super T, ? super Throwable> complete(final CompletableFuture<T> b) {
        return (value, exception) -> {
            if (exception != null)
                b.completeExceptionally(exception);
            else
                b.complete(value);
        };
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link ConsumerNetworkThread}.
     */
    public static Supplier<ApplicationEventProcessor> supplier(final LogContext logContext,
                                                               final ConsumerMetadata metadata,
                                                               final SubscriptionState subscriptions,
                                                               final Supplier<RequestManagers> requestManagersSupplier) {
        return new CachedSupplier<ApplicationEventProcessor>() {
            @Override
            protected ApplicationEventProcessor create() {
                RequestManagers requestManagers = requestManagersSupplier.get();
                return new ApplicationEventProcessor(
                        logContext,
                        requestManagers,
                        metadata,
                        subscriptions
                );
            }
        };
    }
}
