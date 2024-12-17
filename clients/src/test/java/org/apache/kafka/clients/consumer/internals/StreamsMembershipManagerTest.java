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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackCompletedEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StreamsMembershipManagerTest {

    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "test-member-1";
    private static final int MEMBER_EPOCH = 1;

    private static final String SUB_TOPOLOGY_ID_0 = "subtopology-0";
    private static final String SUB_TOPOLOGY_ID_1 = "subtopology-1";

    private static final String TOPIC_0 = "topic-0";
    private static final String TOPIC_1 = "topic-1";

    private static final int PARTITION_0 = 0;
    private static final int PARTITION_1 = 1;

    private Time time = new MockTime(0);
    private Metrics metrics = new Metrics(time);

    private StreamsMembershipManager membershipManager;

    @Mock
    private SubscriptionState subscriptionState;

    @Mock
    private StreamsAssignmentInterface streamsAssignmentInterface;

    @BeforeEach
    public void setup() {
        membershipManager = new StreamsMembershipManager(
            GROUP_ID,
            streamsAssignmentInterface,
            subscriptionState,
            new LogContext("test"),
            time,
            metrics
        );
        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testUnexpectedErrorInHeartbeatResponse() {
        final String errorMessage = "Nobody expects the Spanish Inquisition!";
        final StreamsGroupHeartbeatResponseData responseData = new StreamsGroupHeartbeatResponseData()
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code())
            .setErrorMessage(errorMessage);
        final StreamsGroupHeartbeatResponse response = new StreamsGroupHeartbeatResponse(responseData);

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> membershipManager.onHeartbeatSuccess(response)
        );

        assertEquals(
            "Unexpected error in Heartbeat response. Expected no error, but received: "
                + Errors.GROUP_AUTHORIZATION_FAILED.name()
                + " with message: '" + errorMessage + "'",
            exception.getMessage()
        );
    }

    @Test
    public void testActiveTasksAreNullInHeartbeatResponse() {
        testTasksAreNullInHeartbeatResponse(null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void testStandbyTasksAreNullInHeartbeatResponse() {
        testTasksAreNullInHeartbeatResponse(Collections.emptyList(), null, Collections.emptyList());
    }

    @Test
    public void testWarmupTasksAreNullInHeartbeatResponse() {
        testTasksAreNullInHeartbeatResponse(Collections.emptyList(), Collections.emptyList(), null);
    }

    private void testTasksAreNullInHeartbeatResponse(final List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks,
                                                     final List<StreamsGroupHeartbeatResponseData.TaskIds> standbyTasks,
                                                     final List<StreamsGroupHeartbeatResponseData.TaskIds> warmupTasks) {
        joining();
        final StreamsGroupHeartbeatResponse response = makeHeartbeatResponse(activeTasks, standbyTasks, warmupTasks);

        final IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> membershipManager.onHeartbeatSuccess(response)
        );

        assertEquals(
            "Invalid response data, task collections must be all null or all non-null: " + response.data(),
            exception.getMessage()
        );
    }

    @Test
    public void testJoining() {
        joining();

        verifyInStateJoining(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testReconcilingEmptyToSingleActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingActiveTaskToDifferentActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsAssignmentInterface.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedPartitionsToRevoke = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Collection<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        onTasksRevokedCallbackExecuted.complete(null);
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
    }

    @Test
    public void testReconcilingSingleActiveTaskToAdditionalActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0),
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_0),
            new TopicPartition(TOPIC_0, PARTITION_1)
        );
        final Collection<TopicPartition> expectedNewPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingMultipleActiveTaskToSingleActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0),
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasksToRevoke = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsAssignmentInterface.requestOnTasksRevokedCallbackInvocation(activeTasksToRevoke))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0), new TopicPartition(TOPIC_0, PARTITION_1)));
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedPartitionsToRevoke = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Collection<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Collection<TopicPartition> expectedNewPartitionsToAssign = Collections.emptySet();
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        onTasksRevokedCallbackExecuted.complete(null);
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
    }

    @Test
    public void testReconcilingEmptyToMultipleActiveTaskOfDifferentSubtopologies() {
        setupStreamsAssignmentInterfaceWithTwoSubtopologies(
            SUB_TOPOLOGY_ID_0, TOPIC_0,
            SUB_TOPOLOGY_ID_1, TOPIC_1
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0),
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_1, PARTITION_0)
        );
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(
            SUB_TOPOLOGY_ID_0, List.of(PARTITION_0),
            SUB_TOPOLOGY_ID_1, List.of(PARTITION_0))
        );

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Set.of(
            new TopicPartition(TOPIC_0, PARTITION_0),
            new TopicPartition(TOPIC_1, PARTITION_0)
        );
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingActiveTaskToStandbyTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> standbyTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsAssignmentInterface.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
            makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet()))
        ).thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)))
            .thenReturn(Collections.emptySet());
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedPartitionsToRevoke = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        onTasksRevokedCallbackExecuted.complete(null);
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
    }

    @Test
    public void testReconcilingActiveTaskToWarmupTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> warmupTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsAssignmentInterface.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onTasksRevokedCallbackExecuted);
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
            makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks))
        ).thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)))
            .thenReturn(Collections.emptySet());
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Set<TopicPartition> expectedPartitionsToRevoke = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(
            expectedPartitionsToRevoke,
            expectedFullPartitionsToAssign,
            expectedNewPartitionsToAssign
        );
        onTasksRevokedCallbackExecuted.complete(null);
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
    }

    @Test
    public void testReconcilingEmptyToSingleStandbyTask() {
        final Set<StreamsAssignmentInterface.TaskId> standbyTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingStandbyTaskToDifferentStandbyTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> standbyTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> standbyTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasksSetup, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithStandbyTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingSingleStandbyTaskToAdditionalStandbyTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> standbyTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> standbyTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0),
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasksSetup, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithStandbyTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingMultipleStandbyTaskToSingleStandbyTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> standbyTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0),
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        final Set<StreamsAssignmentInterface.TaskId> standbyTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasksSetup, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithStandbyTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingStandbyTaskToActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> standbyTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasksSetup, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_0)));
        joining();
        reconcile(makeHeartbeatResponseWithStandbyTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingStandbyTaskToWarmupTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> standbyTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> warmupTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasksSetup, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
            makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks))
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithStandbyTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingEmptyToSingleWarmupTask() {
        final Set<StreamsAssignmentInterface.TaskId> warmupTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks)
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingWarmupTaskToDifferentWarmupTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> warmupTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> warmupTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasksSetup)
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks)
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithWarmupTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingSingleWarmupTaskToAdditionalWarmupTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> warmupTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> warmupTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0),
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasksSetup)
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks)
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithWarmupTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingMultipleWarmupTaskToSingleWarmupTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> warmupTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0),
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        final Set<StreamsAssignmentInterface.TaskId> warmupTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasksSetup)
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasks)
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithWarmupTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0, PARTITION_1)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);

        reconcile(makeHeartbeatResponseWithWarmupTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingWarmupTaskToActiveTask() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> warmupTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> activeTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasksSetup)
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        when(subscriptionState.assignedPartitions())
            .thenReturn(Collections.emptySet())
            .thenReturn(Collections.emptySet())
            .thenReturn(Set.of(new TopicPartition(TOPIC_0, PARTITION_1)));
        joining();
        reconcile(makeHeartbeatResponseWithWarmupTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_1));
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingWarmupTaskToStandbyTask() {
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> warmupTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        final Set<StreamsAssignmentInterface.TaskId> standbyTasks = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_1)
        );
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), Collections.emptySet(), warmupTasksSetup)
            )
        ).thenReturn(onTasksAssignedCallbackExecutedSetup);
        when(
            streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(
                makeTaskAssignment(Collections.emptySet(), standbyTasks, Collections.emptySet())
            )
        ).thenReturn(onTasksAssignedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithWarmupTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        Mockito.reset(subscriptionState);

        reconcile(makeHeartbeatResponseWithStandbyTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Collections.emptySet();
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);
        onTasksAssignedCallbackExecuted.complete(null);
        verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(expectedNewPartitionsToAssign);
        verifyThatNoTasksHaveBeenRevoked();
    }

    @Test
    public void testReconcilingAndAssignmentCallbackFails() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecuted);
        joining();

        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));

        final Collection<TopicPartition> expectedFullPartitionsToAssign = Set.of(new TopicPartition(TOPIC_0, PARTITION_0));
        final Collection<TopicPartition> expectedNewPartitionsToAssign = expectedFullPartitionsToAssign;
        verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(expectedFullPartitionsToAssign, expectedNewPartitionsToAssign);

        onTasksAssignedCallbackExecuted.completeExceptionally(new RuntimeException("KABOOM!"));

        verifyInStateReconciling(membershipManager);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(any());
    }

    @Test
    public void testLeaveGroupWhenNotInGroup() {
        testLeaveGroupWhenNotInGroup(membershipManager::leaveGroup);
    }

    @Test
    public void testLeaveGroupOnCloseWhenNotInGroup() {
        testLeaveGroupWhenNotInGroup(membershipManager::leaveGroupOnClose);
    }

    private void testLeaveGroupWhenNotInGroup(final Supplier<CompletableFuture<Void>> leaveGroup) {
        final CompletableFuture<Void> future = leaveGroup.get();

        assertFalse(membershipManager.isLeavingGroup());
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
        verify(subscriptionState).unsubscribe();
        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenNotInGroupAndFenced() {
        testLeaveGroupOnCloseWhenNotInGroupAndFenced(membershipManager::leaveGroup);
    }

    @Test
    public void testLeaveGroupOnCloseWhenNotInGroupAndFenced() {
        testLeaveGroupOnCloseWhenNotInGroupAndFenced(membershipManager::leaveGroupOnClose);
    }

    private void testLeaveGroupOnCloseWhenNotInGroupAndFenced(final Supplier<CompletableFuture<Void>> leaveGroup) {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        fenced();
        final CompletableFuture<Void> future = leaveGroup.get();

        assertFalse(membershipManager.isLeavingGroup());
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
        verify(subscriptionState).unsubscribe();
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testLeaveGroupWhenInGroupWithAssignment() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onTasksRevokedCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksRevokedCallbackInvocation(activeTasks))
            .thenReturn(onTasksRevokedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        final CompletableFuture<Void> onGroupLeft = membershipManager.leaveGroup();

        assertFalse(onGroupLeft.isDone());
        verify(subscriptionState, never()).unsubscribe();
        verifyInStatePrepareLeaving(membershipManager);
        final CompletableFuture<Void> onGroupLeftBeforeRevocationCallback = membershipManager.leaveGroup();
        assertEquals(onGroupLeft, onGroupLeftBeforeRevocationCallback);
        final CompletableFuture<Void> onGroupLeftOnCloseBeforeRevocationCallback = membershipManager.leaveGroupOnClose();
        assertEquals(onGroupLeft, onGroupLeftOnCloseBeforeRevocationCallback);
        onTasksRevokedCallbackExecuted.complete(null);
        verify(subscriptionState).unsubscribe();
        assertFalse(onGroupLeft.isDone());
        verifyInStateLeaving(membershipManager);
        final CompletableFuture<Void> onGroupLeftAfterRevocationCallback = membershipManager.leaveGroup();
        assertEquals(onGroupLeft, onGroupLeftAfterRevocationCallback);
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateUnsubscribed(membershipManager);
        membershipManager.onHeartbeatSuccess(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        assertTrue(onGroupLeft.isDone());
        assertFalse(onGroupLeft.isCompletedExceptionally());
    }

    @Test
    public void testLeaveGroupOnCloseWhenInGroupWithAssignment() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        final CompletableFuture<Void> onGroupLeft = membershipManager.leaveGroupOnClose();

        assertFalse(onGroupLeft.isDone());
        verifyInStateLeaving(membershipManager);
        verify(subscriptionState).unsubscribe();
        verify(streamsAssignmentInterface, never()).requestOnTasksRevokedCallbackInvocation(any());
        final CompletableFuture<Void> onGroupLeftBeforeHeartbeatRequestGenerated = membershipManager.leaveGroup();
        assertEquals(onGroupLeft, onGroupLeftBeforeHeartbeatRequestGenerated);
        final CompletableFuture<Void> onGroupLeftOnCloseBeforeHeartbeatRequestGenerated = membershipManager.leaveGroupOnClose();
        assertEquals(onGroupLeft, onGroupLeftOnCloseBeforeHeartbeatRequestGenerated);
        assertFalse(onGroupLeft.isDone());
        membershipManager.onHeartbeatRequestGenerated();
        verifyInStateUnsubscribed(membershipManager);
        membershipManager.onHeartbeatSuccess(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        assertTrue(onGroupLeft.isDone());
        assertFalse(onGroupLeft.isCompletedExceptionally());
    }

    @Test
    public void testTransitionToUnsubscribeWhenInLeaving() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, "topic");
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksRevokedCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksRevokedCallbackInvocation(activeTasksSetup))
            .thenReturn(onAllTasksRevokedCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        CompletableFuture<Void> future = leaving(onAllTasksRevokedCallbackExecuted);

        membershipManager.transitionToUnsubscribeIfLeaving();

        verifyInStateUnsubscribed(membershipManager);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testOnPollTimerExpired() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        membershipManager.onPollTimerExpired();

        verifyInStateLeaving(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInAcknowleding() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStable(membershipManager);
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInAcknowledgingAndNewTargetAssignment() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_1)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateReconciling(membershipManager);
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        leaving(onAllTasksLostCallbackExecuted);

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateUnsubscribed(membershipManager);
    }

    @Test
    public void testOnHeartbeatRequestGeneratedWhenInLeavingAndPollTimerExpired() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        membershipManager.onPollTimerExpired();

        membershipManager.onHeartbeatRequestGenerated();

        verifyInStateStale(membershipManager);
    }

    @Test
    public void testOnFencedWhenInJoining() {
        joining();

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    @Test
    public void testOnFencedWhenInReconciling() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    @Test
    public void testOnFencedWhenInAcknowledging() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    @Test
    public void testOnFencedWhenInStable() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final Set<StreamsAssignmentInterface.TaskId> activeTasks =
            Set.of(new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0));
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasks, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        stable();

        testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable();
    }

    private void testOnFencedWhenInJoiningOrReconcilingOrAcknowledgingOrStable() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);

        membershipManager.onFenced();

        verifyInStateFenced(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
        onAllTasksLostCallbackExecuted.complete(null);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        verifyInStateJoining(membershipManager);
    }

    @Test
    public void testOnFencedWhenInPrepareLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testOnFencedWhenInPrepareLeavingOrLeaving(prepareLeaving());
    }

    @Test
    public void testOnFencedWhenInLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testOnFencedWhenInPrepareLeavingOrLeaving(leaving(onAllTasksLostCallbackExecuted));
    }

    private void testOnFencedWhenInPrepareLeavingOrLeaving(final CompletableFuture<Void> onGroupLeft) {
        membershipManager.onFenced();

        verifyInStateUnsubscribed(membershipManager);
        assertEquals(StreamsGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH, membershipManager.memberEpoch());
        assertTrue(onGroupLeft.isDone());
        assertFalse(onGroupLeft.isCancelled());
        assertFalse(onGroupLeft.isCompletedExceptionally());
    }

    @Test
    public void testTransitionToFatalWhenInPrepareLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testTransitionToFatalWhenInPrepareLeavingOrLeaving(prepareLeaving());
    }

    @Test
    public void testTransitionToFatalWhenInLeaving() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testTransitionToFatalWhenInPrepareLeavingOrLeaving(leaving(onAllTasksLostCallbackExecuted));
    }

    private void testTransitionToFatalWhenInPrepareLeavingOrLeaving(final CompletableFuture<Void> onGroupLeft) {
        membershipManager.transitionToFatal();

        verifyInStateFatal(membershipManager);
        assertTrue(onGroupLeft.isDone());
        assertFalse(onGroupLeft.isCancelled());
        assertFalse(onGroupLeft.isCompletedExceptionally());
    }

    @Test
    public void testTransitionToFatalWhenInJoining() {
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    @Test
    public void testTransitionToFatalWhenInReconciling() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    @Test
    public void testTransitionToFatalWhenInAcknowledging() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    @Test
    public void testTransitionToFatalWhenInStable() {
        setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(SUB_TOPOLOGY_ID_0, TOPIC_0);
        final CompletableFuture<Void> onTasksAssignedCallbackExecutedSetup = new CompletableFuture<>();
        final Set<StreamsAssignmentInterface.TaskId> activeTasksSetup = Set.of(
            new StreamsAssignmentInterface.TaskId(SUB_TOPOLOGY_ID_0, PARTITION_0)
        );
        when(streamsAssignmentInterface.requestOnTasksAssignedCallbackInvocation(makeTaskAssignment(activeTasksSetup, Collections.emptySet(), Collections.emptySet())))
            .thenReturn(onTasksAssignedCallbackExecutedSetup);
        final CompletableFuture<Void> onAllTasksLostCallbackExecuted = new CompletableFuture<>();
        when(streamsAssignmentInterface.requestOnAllTasksLostCallbackInvocation())
            .thenReturn(onAllTasksLostCallbackExecuted);
        joining();
        reconcile(makeHeartbeatResponseWithActiveTasks(SUB_TOPOLOGY_ID_0, List.of(PARTITION_0)));
        acknowledging(onTasksAssignedCallbackExecutedSetup);
        stable();

        testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(onAllTasksLostCallbackExecuted);
    }

    private void testTransitionToFatalWhenInJoiningOrReconcilingOrAcknowledgingOrStable(final CompletableFuture<Void> future) {
        membershipManager.transitionToFatal();

        verify(subscriptionState, never()).assignFromSubscribed(Collections.emptySet());
        future.complete(null);
        verify(subscriptionState).assignFromSubscribed(Collections.emptySet());
        verifyInStateFatal(membershipManager);
    }

    @Test
    public void testOnTasksAssignedCallbackCompleted() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnTasksAssignedCallbackCompletedEvent event = new StreamsOnTasksAssignedCallbackCompletedEvent(
            future,
            Optional.empty()
        );

        membershipManager.onTasksAssignedCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testOnAllTasksLostCallbackCompleted() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnAllTasksLostCallbackCompletedEvent event = new StreamsOnAllTasksLostCallbackCompletedEvent(
            future,
            Optional.empty()
        );

        membershipManager.onAllTasksLostCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testOnTasksAssignedCallbackCompletedWhenCallbackFails() {
        final String errorMessage = "KABOOM!";
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final StreamsOnAllTasksLostCallbackCompletedEvent event = new StreamsOnAllTasksLostCallbackCompletedEvent(
            future,
            Optional.of(new KafkaException(errorMessage))
        );

        membershipManager.onAllTasksLostCallbackCompleted(event);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
        final ExecutionException executionException = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(KafkaException.class, executionException.getCause());
        assertEquals(errorMessage, executionException.getCause().getMessage());
    }

    private void verifyThatNoTasksHaveBeenRevoked() {
        verify(streamsAssignmentInterface, never()).requestOnTasksRevokedCallbackInvocation(any());
        verify(subscriptionState, never()).markPendingRevocation(any());
    }

    private void verifyInStateReconcilingBeforeOnTaskRevokedCallbackExecuted(Set<TopicPartition> expectedPartitionsToRevoke,
                                                                             Collection<TopicPartition> expectedAllPartitionsToAssign,
                                                                             Collection<TopicPartition> expectedNewPartitionsToAssign) {
        verify(subscriptionState).markPendingRevocation(expectedPartitionsToRevoke);
        verify(subscriptionState, never()).assignFromSubscribedAwaitingCallback(expectedAllPartitionsToAssign, expectedNewPartitionsToAssign);
        verifyInStateReconciling(membershipManager);
    }

    private void verifyInStateReconcilingBeforeOnTaskAssignedCallbackExecuted(Collection<TopicPartition> expectedAllPartitionsToAssign,
                                                                              Collection<TopicPartition> expectedNewPartitionsToAssign) {
        verify(subscriptionState).assignFromSubscribedAwaitingCallback(expectedAllPartitionsToAssign, expectedNewPartitionsToAssign);
        verify(subscriptionState, never()).enablePartitionsAwaitingCallback(expectedNewPartitionsToAssign);
        verifyInStateReconciling(membershipManager);
    }

    private void verifyInStateAcknowledgingAfterOnTaskAssignedCallbackExecuted(Collection<TopicPartition> expectedNewPartitionsToAssign) {
        verify(subscriptionState).enablePartitionsAwaitingCallback(expectedNewPartitionsToAssign);
        verifyInStateAcknowledging(membershipManager);
    }

    private static void verifyInStateReconciling(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.RECONCILING, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateAcknowledging(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.ACKNOWLEDGING, membershipManager.state());
        assertTrue(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateLeaving(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.LEAVING, membershipManager.state());
        assertTrue(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertTrue(membershipManager.isLeavingGroup());
    }

    private static void verifyInStatePrepareLeaving(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.PREPARE_LEAVING, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertTrue(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateUnsubscribed(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.UNSUBSCRIBED, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateJoining(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.JOINING, membershipManager.state());
        assertTrue(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateStable(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.STABLE, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertFalse(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateFenced(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.FENCED, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateFatal(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.FATAL, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private static void verifyInStateStale(final StreamsMembershipManager membershipManager) {
        assertEquals(MemberState.STALE, membershipManager.state());
        assertFalse(membershipManager.shouldHeartbeatNow());
        assertTrue(membershipManager.shouldSkipHeartbeat());
        assertFalse(membershipManager.isLeavingGroup());
    }

    private void setupStreamsAssignmentInterfaceWithOneSubtopologyOneSourceTopic(final String subtopologyId,
                                                                                 final String topicName) {
        when(streamsAssignmentInterface.subtopologyMap()).thenReturn(
            mkMap(
                mkEntry(
                    subtopologyId,
                    new StreamsAssignmentInterface.Subtopology(
                        Set.of(topicName),
                        Collections.emptySet(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyList()
                    )
                )
            )
        );
    }

    private void setupStreamsAssignmentInterfaceWithTwoSubtopologies(final String subtopologyId1,
                                                                     final String topicName1,
                                                                     final String subtopologyId2,
                                                                     final String topicName2) {
        when(streamsAssignmentInterface.subtopologyMap()).thenReturn(
            mkMap(
                mkEntry(
                    subtopologyId1,
                    new StreamsAssignmentInterface.Subtopology(
                        Set.of(topicName1),
                        Collections.emptySet(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyList()
                    )
                ),
                mkEntry(
                    subtopologyId2,
                    new StreamsAssignmentInterface.Subtopology(
                        Set.of(topicName2),
                        Collections.emptySet(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyList()
                    )
                )
            )
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithActiveTasks(final String subtopologyId,
                                                                               final List<Integer> partitions) {
        return makeHeartbeatResponseWithActiveTasks(List.of(
            new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(subtopologyId)
                .setPartitions(partitions)
        ));
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithStandbyTasks(final String subtopologyId,
                                                                                final List<Integer> partitions) {
        return makeHeartbeatResponse(
            Collections.emptyList(),
            List.of(
                new StreamsGroupHeartbeatResponseData.TaskIds()
                    .setSubtopologyId(subtopologyId)
                    .setPartitions(partitions)
            ),
            Collections.emptyList()
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithWarmupTasks(final String subtopologyId,
                                                                               final List<Integer> partitions) {
        return makeHeartbeatResponse(
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(
                new StreamsGroupHeartbeatResponseData.TaskIds()
                    .setSubtopologyId(subtopologyId)
                    .setPartitions(partitions)
            )
        );
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithActiveTasks(final String subtopologyId0,
                                                                               final List<Integer> partitions0,
                                                                               final String subtopologyId1,
                                                                               final List<Integer> partitions1) {
        return makeHeartbeatResponseWithActiveTasks(List.of(
            new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(subtopologyId0)
                .setPartitions(partitions0),
            new StreamsGroupHeartbeatResponseData.TaskIds()
                .setSubtopologyId(subtopologyId1)
                .setPartitions(partitions1)
        ));
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponseWithActiveTasks(final List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks) {
        return makeHeartbeatResponse(activeTasks, Collections.emptyList(), Collections.emptyList());
    }

    private StreamsGroupHeartbeatResponse makeHeartbeatResponse(final List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks,
                                                                final List<StreamsGroupHeartbeatResponseData.TaskIds> standbyTasks,
                                                                final List<StreamsGroupHeartbeatResponseData.TaskIds> warmupTasks) {
        final StreamsGroupHeartbeatResponseData responseData = new StreamsGroupHeartbeatResponseData()
            .setErrorCode(Errors.NONE.code())
            .setMemberId(MEMBER_ID)
            .setMemberEpoch(MEMBER_EPOCH)
            .setActiveTasks(activeTasks)
            .setStandbyTasks(standbyTasks)
            .setWarmupTasks(warmupTasks);
        return new StreamsGroupHeartbeatResponse(responseData);
    }

    private StreamsAssignmentInterface.Assignment makeTaskAssignment(final Set<StreamsAssignmentInterface.TaskId> activeTasks,
                                                                     final Set<StreamsAssignmentInterface.TaskId> standbyTasks,
                                                                     final Set<StreamsAssignmentInterface.TaskId> warmupTasks) {
        return new StreamsAssignmentInterface.Assignment(
            activeTasks,
            standbyTasks,
            warmupTasks
        );
    }

    private void joining() {
        membershipManager.onSubscriptionUpdated();
        membershipManager.onConsumerPoll();
        verifyInStateJoining(membershipManager);
    }

    private void reconcile(final StreamsGroupHeartbeatResponse response) {
        membershipManager.onHeartbeatSuccess(response);
        membershipManager.poll(time.milliseconds());
        verifyInStateReconciling(membershipManager);

    }

    private void acknowledging(final CompletableFuture<Void> future) {
        future.complete(null);
        verifyInStateAcknowledging(membershipManager);
    }

    private CompletableFuture<Void> prepareLeaving() {
        final CompletableFuture<Void> onGroupLeft = membershipManager.leaveGroup();
        verifyInStatePrepareLeaving(membershipManager);
        return onGroupLeft;
    }

    private CompletableFuture<Void> leaving(final CompletableFuture<Void> onAllTasksRevokedCallbackExecuted) {
        final CompletableFuture<Void> future = prepareLeaving();
        onAllTasksRevokedCallbackExecuted.complete(null);
        verifyInStateLeaving(membershipManager);
        return future;
    }

    private void stable() {
        membershipManager.onHeartbeatRequestGenerated();
    }

    private void fenced() {
        membershipManager.onFenced();
    }
}
