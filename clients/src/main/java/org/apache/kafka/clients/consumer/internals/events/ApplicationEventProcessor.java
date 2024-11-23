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
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private int metadataVersionSnapshot;

    public ApplicationEventProcessor(final LogContext logContext,
                                     final RequestManagers requestManagers,
                                     final ConsumerMetadata metadata,
                                     final SubscriptionState subscriptions) {
        this.log = logContext.logger(ApplicationEventProcessor.class);
        this.requestManagers = requestManagers;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.metadataVersionSnapshot = metadata.updateVersion();
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

            case RESET_OFFSET:
                process((ResetOffsetEvent) event);
                return;

            case CHECK_AND_UPDATE_POSITIONS:
                process((CheckAndUpdatePositionsEvent) event);
                return;

            case TOPIC_SUBSCRIPTION_CHANGE:
                process((TopicSubscriptionChangeEvent) event);
                return;

            case TOPIC_PATTERN_SUBSCRIPTION_CHANGE:
                process((TopicPatternSubscriptionChangeEvent) event);
                return;

            case UPDATE_SUBSCRIPTION_METADATA:
                process((UpdatePatternSubscriptionEvent) event);
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

            case LEAVE_GROUP_ON_CLOSE:
                process((LeaveGroupOnCloseEvent) event);
                return;

            case CREATE_FETCH_REQUESTS:
                process((CreateFetchRequestsEvent) event);
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

            case SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK_REGISTRATION:
                process((ShareAcknowledgementCommitCallbackRegistrationEvent) event);
                return;

            case SEEK_UNVALIDATED:
                process((SeekUnvalidatedEvent) event);
                return;

            default:
                log.warn("Application event type {} was not expected", event.type());
        }
    }

    private void process(final PollEvent event) {
        if (requestManagers.commitRequestManager.isPresent()) {
            requestManagers.commitRequestManager.ifPresent(m -> m.updateAutoCommitTimer(event.pollTimeMs()));
            requestManagers.consumerHeartbeatRequestManager.ifPresent(hrm -> {
                hrm.membershipManager().onConsumerPoll();
                hrm.resetPollTimer(event.pollTimeMs());
            });
        } else {
            requestManagers.shareHeartbeatRequestManager.ifPresent(hrm -> {
                hrm.membershipManager().onConsumerPoll();
                hrm.resetPollTimer(event.pollTimeMs());
            });
        }
    }

    private void process(final CreateFetchRequestsEvent event) {
        CompletableFuture<Void> future = requestManagers.fetchRequestManager.createFetchRequests();
        future.whenComplete(complete(event.future()));
    }

    private void process(final AsyncCommitEvent event) {
        if (requestManagers.commitRequestManager.isEmpty()) {
            event.future().completeExceptionally(new KafkaException("Unable to async commit " +
                "offset because the CommitRequestManager is not available. Check if group.id was set correctly"));
            return;
        }

        try {
            CommitRequestManager manager = requestManagers.commitRequestManager.get();
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = manager.commitAsync(event.offsets());
            future.whenComplete(complete(event.future()));
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    private void process(final SyncCommitEvent event) {
        
        if (requestManagers.commitRequestManager.isEmpty()) {
            event.future().completeExceptionally(new KafkaException("Unable to sync commit " +
                "offset because the CommitRequestManager is not available. Check if group.id was set correctly"));
            return;
        }

        try {
            CommitRequestManager manager = requestManagers.commitRequestManager.get();
            CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = manager.commitSync(event.offsets(), event.deadlineMs());
            future.whenComplete(complete(event.future()));
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    private void process(final FetchCommittedOffsetsEvent event) {
        if (requestManagers.commitRequestManager.isEmpty()) {
            event.future().completeExceptionally(new KafkaException("Unable to fetch committed " +
                    "offset because the CommitRequestManager is not available. Check if group.id was set correctly"));
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
     * Process event that indicates that the subscription topics changed. This will make the
     * consumer join the group if it is not part of it yet, or send the updated subscription if
     * it is already a member on the next poll.
     */
    private void process(final TopicSubscriptionChangeEvent event) {
        if (requestManagers.consumerHeartbeatRequestManager.isEmpty()) {
            log.warn("Group membership manager not present when processing a subscribe event");
            event.future().complete(null);
            return;
        }

        try {
            if (subscriptions.subscribe(event.topics(), event.listener()))
                this.metadataVersionSnapshot = metadata.requestUpdateForNewTopics();

            // Join the group if not already part of it, or just send the new subscription to the broker on the next poll.
            requestManagers.consumerHeartbeatRequestManager.get().membershipManager().onSubscriptionUpdated();
            event.future().complete(null);
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    /**
     * Process event that indicates that the subscription topic pattern changed. This will make the
     * consumer join the group if it is not part of it yet, or send the updated subscription if
     * it is already a member on the next poll.
     */
    private void process(final TopicPatternSubscriptionChangeEvent event) {
        try {
            subscriptions.subscribe(event.pattern(), event.listener());
            metadata.requestUpdateForNewTopics();
            updatePatternSubscription(metadata.fetch());
            event.future().complete(null);
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    /**
     * Process event that re-evaluates the subscribed regular expression using the latest topics from metadata, only if metadata changed.
     * This will make the consumer send the updated subscription on the next poll.
     */
    private void process(final UpdatePatternSubscriptionEvent event) {
        if (this.metadataVersionSnapshot < metadata.updateVersion()) {
            this.metadataVersionSnapshot = metadata.updateVersion();
            if (subscriptions.hasPatternSubscription()) {
                updatePatternSubscription(metadata.fetch());
            }
        }
        event.future().complete(null);
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

    private void process(final ResetOffsetEvent event) {
        try {
            Collection<TopicPartition> parts = event.topicPartitions().isEmpty() ?
                    subscriptions.assignedPartitions() : event.topicPartitions();
            subscriptions.requestOffsetReset(parts, event.offsetResetStrategy());
            event.future().complete(null);
        } catch (Exception e) {
            event.future().completeExceptionally(e);
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
        if (requestManagers.consumerHeartbeatRequestManager.isEmpty()) {
            log.warn(
                "An internal error occurred; the group membership manager was not present, so the notification of the {} callback execution could not be sent",
                event.methodName()
            );
            return;
        }
        requestManagers.consumerHeartbeatRequestManager.get().membershipManager().consumerRebalanceListenerCallbackCompleted(event);
    }

    private void process(@SuppressWarnings("unused") final CommitOnCloseEvent event) {
        if (requestManagers.commitRequestManager.isEmpty())
            return;
        log.debug("Signal CommitRequestManager closing");
        requestManagers.commitRequestManager.get().signalClose();
    }

    private void process(final LeaveGroupOnCloseEvent event) {
        if (requestManagers.consumerMembershipManager.isEmpty())
            return;

        log.debug("Signal the ConsumerMembershipManager to leave the consumer group since the consumer is closing");
        CompletableFuture<Void> future = requestManagers.consumerMembershipManager.get().leaveGroupOnClose();
        future.whenComplete(complete(event.future()));
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
        if (requestManagers.shareConsumeRequestManager.isEmpty()) {
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
        if (requestManagers.shareConsumeRequestManager.isEmpty()) {
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
    private void process(final ShareSubscriptionChangeEvent event) {
        if (requestManagers.shareHeartbeatRequestManager.isEmpty()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing a subscribe event");
            event.future().completeExceptionally(error);
            return;
        }

        if (subscriptions.subscribeToShareGroup(event.topics()))
            metadata.requestUpdateForNewTopics();

        requestManagers.shareHeartbeatRequestManager.get().membershipManager().onSubscriptionUpdated();

        event.future().complete(null);
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
        if (requestManagers.shareHeartbeatRequestManager.isEmpty()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing an unsubscribe event");
            event.future().completeExceptionally(error);
            return;
        }

        subscriptions.unsubscribe();

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
        if (requestManagers.shareConsumeRequestManager.isEmpty()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing an acknowledge-on-close event");
            event.future().completeExceptionally(error);
            return;
        }

        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        CompletableFuture<Void> future = manager.acknowledgeOnClose(event.acknowledgementsMap(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    /**
     * Process event indicating whether the AcknowledgeCommitCallbackHandler is configured by the user.
     *
     * @param event Event containing a boolean to indicate if the callback handler is configured or not.
     */
    private void process(final ShareAcknowledgementCommitCallbackRegistrationEvent event) {
        if (requestManagers.shareConsumeRequestManager.isEmpty()) {
            return;
        }

        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        manager.setAcknowledgementCommitCallbackRegistered(event.isCallbackRegistered());
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
        return new CachedSupplier<>() {
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

    private void process(final SeekUnvalidatedEvent event) {
        try {
            event.offsetEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(event.partition(), epoch));
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    event.offset(),
                    event.offsetEpoch(),
                    metadata.currentLeader(event.partition())
            );
            subscriptions.seekUnvalidated(event.partition(), newPosition);
            event.future().complete(null);
        } catch (Exception e) {
            event.future().completeExceptionally(e);
        }
    }

    /**
     * This function evaluates the regex that the consumer subscribed to
     * against the list of topic names from metadata, and updates
     * the list of topics in subscription state accordingly
     *
     * @param cluster Cluster from which we get the topics
     */
    private void updatePatternSubscription(Cluster cluster) {
        if (requestManagers.consumerHeartbeatRequestManager.isEmpty()) {
            log.warn("Group membership manager not present when processing a subscribe event");
            return;
        }
        final Set<String> topicsToSubscribe = cluster.topics().stream()
            .filter(subscriptions::matchesSubscribedPattern)
            .collect(Collectors.toSet());
        if (subscriptions.subscribeFromPattern(topicsToSubscribe)) {
            this.metadataVersionSnapshot = metadata.requestUpdateForNewTopics();

            // Join the group if not already part of it, or just send the new subscription to the broker on the next poll.
            requestManagers.consumerHeartbeatRequestManager.get().membershipManager().onSubscriptionUpdated();
        }
    }

    // Visible for testing
    int metadataVersionSnapshot() {
        return metadataVersionSnapshot;
    }
    
}
