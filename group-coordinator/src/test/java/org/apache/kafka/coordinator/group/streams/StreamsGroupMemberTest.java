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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue.TaskIds;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue.KeyValue;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksPerSubtopology;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StreamsGroupMemberTest {

    @Test
    public void testNewMemberDefaults() {
        final String memberId = Uuid.randomUuid().toString();
        StreamsGroupMember member = new StreamsGroupMember.Builder(memberId).build();

        assertEquals(memberId, member.memberId());
        assertEquals(0, member.memberEpoch());
        assertEquals(-1, member.previousMemberEpoch());
        assertEquals(MemberState.STABLE, member.state());
        assertNull(member.instanceId());
        assertNull(member.rackId());
        assertEquals(-1, member.rebalanceTimeoutMs());
        assertEquals("", member.clientId());
        assertEquals("", member.clientHost());
        assertEquals(-1, member.topologyEpoch());
        assertNull(member.processId());
        assertNull(member.userEndpoint());
        assertEquals(Collections.emptyMap(), member.clientTags());
        assertEquals(Collections.emptyMap(), member.assignedActiveTasks());
        assertEquals(Collections.emptyMap(), member.assignedStandbyTasks());
        assertEquals(Collections.emptyMap(), member.assignedWarmupTasks());
        assertEquals(Collections.emptyMap(), member.activeTasksPendingRevocation());
        assertEquals(Collections.emptyMap(), member.standbyTasksPendingRevocation());
        assertEquals(Collections.emptyMap(), member.warmupTasksPendingRevocation());
    }

    @Test
    public void testNewMember() {
        final String memberId = "member-id";
        final int memberEpoch = 10;
        final int previousMemberEpoch = 9;
        final MemberState state = MemberState.UNRELEASED_TASKS;
        final String instanceId = "instance-id";
        final String rackId = "rack-id";
        final int rebalanceTimeout = 5000;
        final String clientId = "client-id";
        final String hostname = "hostname";
        final int topologyEpoch = 3;
        final String processId = "process-id";
        final String subtopology1 = "subtopology1";
        final String subtopology2 = "subtopology2";
        final StreamsGroupMemberMetadataValue.Endpoint userEndpoint =
            new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090);
        final Map<String, String> clientTags = mkMap(mkEntry("client", "tag"));
        final Map<String, Set<Integer>> assignedActiveTasks = mkTasksPerSubtopology(mkTasks(subtopology1, 1, 2, 3));
        final Map<String, Set<Integer>> assignedStandbyTasks = mkTasksPerSubtopology(mkTasks(subtopology2, 6, 5, 4));
        final Map<String, Set<Integer>> assignedWarmupTasks = mkTasksPerSubtopology(mkTasks(subtopology1, 7, 8, 9));
        final Map<String, Set<Integer>> activeTasksPendingRevocation = mkTasksPerSubtopology(mkTasks(subtopology2, 3, 2, 1));
        final Map<String, Set<Integer>> standbyTasksPendingRevocation = mkTasksPerSubtopology(mkTasks(subtopology1, 4, 5, 6));
        final Map<String, Set<Integer>> warmupTasksPendingRevocation = mkTasksPerSubtopology(mkTasks(subtopology2, 9, 8, 7));
        StreamsGroupMember member = new StreamsGroupMember.Builder(memberId)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(previousMemberEpoch)
            .setState(state)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setRebalanceTimeoutMs(rebalanceTimeout)
            .setClientId(clientId)
            .setClientHost(hostname)
            .setTopologyEpoch(topologyEpoch)
            .setProcessId(processId)
            .setUserEndpoint(userEndpoint)
            .setClientTags(clientTags)
            .setAssignedActiveTasks(assignedActiveTasks)
            .setAssignedStandbyTasks(assignedStandbyTasks)
            .setAssignedWarmupTasks(assignedWarmupTasks)
            .setActiveTasksPendingRevocation(activeTasksPendingRevocation)
            .setStandbyTasksPendingRevocation(standbyTasksPendingRevocation)
            .setWarmupTasksPendingRevocation(warmupTasksPendingRevocation)
            .build();

        assertEquals(memberId, member.memberId());
        assertEquals(memberEpoch, member.memberEpoch());
        assertEquals(previousMemberEpoch, member.previousMemberEpoch());
        assertEquals(state, member.state());
        assertEquals(instanceId, member.instanceId());
        assertEquals(rackId, member.rackId());
        assertEquals(clientId, member.clientId());
        assertEquals(hostname, member.clientHost());
        assertEquals(topologyEpoch, member.topologyEpoch());
        assertEquals(processId, member.processId());
        assertEquals(userEndpoint, member.userEndpoint());
        assertEquals(clientTags, member.clientTags());
        assertEquals(assignedActiveTasks, member.assignedActiveTasks());
        assertEquals(assignedStandbyTasks, member.assignedStandbyTasks());
        assertEquals(assignedWarmupTasks, member.assignedWarmupTasks());
        assertEquals(activeTasksPendingRevocation, member.activeTasksPendingRevocation());
        assertEquals(standbyTasksPendingRevocation, member.standbyTasksPendingRevocation());
        assertEquals(warmupTasksPendingRevocation, member.warmupTasksPendingRevocation());
    }

    @Test
    public void testEquals() {
        StreamsGroupMember member1 = new StreamsGroupMember.Builder("member-id").build();
        StreamsGroupMember member2 = new StreamsGroupMember.Builder(member1.memberId()).build();
        assertEquals(member1, member2);
        assertEquals(member1.hashCode(), member2.hashCode());

        final StreamsGroupMember member3 = new StreamsGroupMember.Builder(member1.memberId() + "2")
            .build();
        assertNotEquals(member1, member3);
        assertNotEquals(member1.hashCode(), member3.hashCode());

        final StreamsGroupMember member4 = new StreamsGroupMember.Builder(member1.memberId())
            .setMemberEpoch(member1.memberEpoch() + 1)
            .build();
        assertNotEquals(member1, member4);
        assertNotEquals(member1.hashCode(), member4.hashCode());

        final StreamsGroupMember member5 = new StreamsGroupMember.Builder(member1.memberId())
            .setPreviousMemberEpoch(member1.previousMemberEpoch() + 1)
            .build();
        assertNotEquals(member1, member5);
        assertNotEquals(member1.hashCode(), member5.hashCode());

        final StreamsGroupMember member6 = new StreamsGroupMember.Builder(member1.memberId())
            .setState(MemberState.UNREVOKED_TASKS)
            .build();
        assertNotEquals(member1, member6);
        assertNotEquals(member1.hashCode(), member6.hashCode());

        final StreamsGroupMember member7 = new StreamsGroupMember.Builder(member1.memberId())
            .setInstanceId("instance-id")
            .build();
        final StreamsGroupMember member8 = new StreamsGroupMember.Builder(member7.memberId())
            .setInstanceId(member7.instanceId())
            .build();
        final StreamsGroupMember member9 = new StreamsGroupMember.Builder(member7.memberId())
            .setInstanceId(member7.instanceId() + "2")
            .build();
        assertEquals(member7, member8);
        assertEquals(member7.hashCode(), member8.hashCode());
        assertNotEquals(member7, member9);
        assertNotEquals(member7.hashCode(), member9.hashCode());

        final StreamsGroupMember member10 = new StreamsGroupMember.Builder(member1.memberId())
            .setRackId("rack-id")
            .build();
        final StreamsGroupMember member11 = new StreamsGroupMember.Builder(member10.memberId())
            .setRackId(member10.rackId())
            .build();
        final StreamsGroupMember member12 = new StreamsGroupMember.Builder(member11.memberId())
            .setRackId(member10.rackId() + "2")
            .build();
        assertEquals(member10, member11);
        assertEquals(member10.hashCode(), member11.hashCode());
        assertNotEquals(member10, member12);
        assertNotEquals(member10.hashCode(), member12.hashCode());

        final StreamsGroupMember member13 = new StreamsGroupMember.Builder(member1.memberId())
            .setClientHost("hostname")
            .build();
        final StreamsGroupMember member14 = new StreamsGroupMember.Builder(member13.memberId())
            .setClientHost(member13.clientHost())
            .build();
        final StreamsGroupMember member15 = new StreamsGroupMember.Builder(member13.memberId())
            .setClientHost(member13.clientHost() + "2")
            .build();
        assertEquals(member13, member14);
        assertEquals(member13.hashCode(), member14.hashCode());
        assertNotEquals(member13, member15);
        assertNotEquals(member13.hashCode(), member15.hashCode());

        final StreamsGroupMember member16 = new StreamsGroupMember.Builder(member1.memberId())
            .setClientId("hostname")
            .build();
        final StreamsGroupMember member17 = new StreamsGroupMember.Builder(member16.memberId())
            .setClientId(member16.clientId())
            .build();
        final StreamsGroupMember member18 = new StreamsGroupMember.Builder(member16.memberId())
            .setClientId(member16.clientId() + "2")
            .build();
        assertEquals(member16, member17);
        assertEquals(member16.hashCode(), member17.hashCode());
        assertNotEquals(member16, member18);
        assertNotEquals(member16.hashCode(), member18.hashCode());

        final StreamsGroupMember member19 = new StreamsGroupMember.Builder(member1.memberId())
            .setRebalanceTimeoutMs(member1.rebalanceTimeoutMs() + 1)
            .build();
        assertNotEquals(member1, member19);
        assertNotEquals(member1.hashCode(), member19.hashCode());

        final StreamsGroupMember member20 = new StreamsGroupMember.Builder(member1.memberId())
            .setTopologyEpoch(member1.topologyEpoch() + 1)
            .build();
        assertNotEquals(member1, member20);
        assertNotEquals(member1.hashCode(), member20.hashCode());

        final StreamsGroupMember member21 = new StreamsGroupMember.Builder(member1.memberId())
            .setProcessId("process-id")
            .build();
        final StreamsGroupMember member22 = new StreamsGroupMember.Builder(member21.memberId())
            .setProcessId(member21.processId())
            .build();
        final StreamsGroupMember member23 = new StreamsGroupMember.Builder(member21.memberId())
            .setClientId(member21.processId() + "2")
            .build();
        assertEquals(member21, member22);
        assertEquals(member21.hashCode(), member22.hashCode());
        assertNotEquals(member21, member23);
        assertNotEquals(member21.hashCode(), member23.hashCode());

        final StreamsGroupMember member24 = new StreamsGroupMember.Builder(member1.memberId())
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
            .build();
        final StreamsGroupMember member25 = new StreamsGroupMember.Builder(member24.memberId())
            .setUserEndpoint(member24.userEndpoint())
            .build();
        final StreamsGroupMember member26 = new StreamsGroupMember.Builder(member24.memberId())
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint()
                .setHost("host")
                .setPort(member24.userEndpoint().port() + 1)
            ).build();
        assertEquals(member24, member25);
        assertEquals(member24.hashCode(), member25.hashCode());
        assertNotEquals(member24, member26);
        assertNotEquals(member24.hashCode(), member26.hashCode());

        final StreamsGroupMember member27 = new StreamsGroupMember.Builder(member1.memberId())
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .build();
        final StreamsGroupMember member28 = new StreamsGroupMember.Builder(member27.memberId())
            .setClientTags(member27.clientTags())
            .build();
        final Map<String, String> clientTags = new HashMap<>(member27.clientTags());
        clientTags.put("client2", "tag2");
        final StreamsGroupMember member29 = new StreamsGroupMember.Builder(member27.memberId())
            .setClientTags(clientTags)
            .build();
        assertEquals(member27, member28);
        assertEquals(member27.hashCode(), member28.hashCode());
        assertNotEquals(member27, member29);
        assertNotEquals(member27.hashCode(), member29.hashCode());

        final StreamsGroupMember member30 = new StreamsGroupMember.Builder(member1.memberId())
            .build();
        final StreamsGroupMember member31 = new StreamsGroupMember.Builder(member30.memberId())
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks("subtopology-id", 1, 2, 3)))
            .build();
        assertEquals(member1, member30);
        assertEquals(member1.hashCode(), member30.hashCode());
        assertNotEquals(member1, member31);
        assertNotEquals(member1.hashCode(), member31.hashCode());

        final StreamsGroupMember member32 = new StreamsGroupMember.Builder(member1.memberId())
            .build();
        final StreamsGroupMember member33 = new StreamsGroupMember.Builder(member32.memberId())
            .setAssignedStandbyTasks(mkTasksPerSubtopology(mkTasks("subtopology-id", 1, 2, 3)))
            .build();
        assertEquals(member1, member32);
        assertEquals(member1.hashCode(), member32.hashCode());
        assertNotEquals(member1, member33);
        assertNotEquals(member1.hashCode(), member33.hashCode());

        final StreamsGroupMember member34 = new StreamsGroupMember.Builder(member1.memberId())
            .build();
        final StreamsGroupMember member35 = new StreamsGroupMember.Builder(member34.memberId())
            .setAssignedWarmupTasks(mkTasksPerSubtopology(mkTasks("subtopology-id", 1, 2, 3)))
            .build();
        assertEquals(member1, member34);
        assertEquals(member1.hashCode(), member34.hashCode());
        assertNotEquals(member1, member35);
        assertNotEquals(member1.hashCode(), member35.hashCode());

        final StreamsGroupMember member36 = new StreamsGroupMember.Builder(member1.memberId())
            .build();
        final StreamsGroupMember member37 = new StreamsGroupMember.Builder(member36.memberId())
            .setActiveTasksPendingRevocation(
                mkTasksPerSubtopology(mkTasks("subtopology-id", 1, 2, 3))
            ).build();
        assertEquals(member1, member36);
        assertEquals(member1.hashCode(), member36.hashCode());
        assertNotEquals(member1, member37);
        assertNotEquals(member1.hashCode(), member37.hashCode());

        final StreamsGroupMember member38 = new StreamsGroupMember.Builder(member1.memberId())
            .build();
        final StreamsGroupMember member39 = new StreamsGroupMember.Builder(member38.memberId())
            .setStandbyTasksPendingRevocation(
                mkTasksPerSubtopology(mkTasks("subtopology-id", 1, 2, 3))
            ).build();
        assertEquals(member1, member38);
        assertEquals(member1.hashCode(), member38.hashCode());
        assertNotEquals(member1, member39);
        assertNotEquals(member1.hashCode(), member39.hashCode());

        final StreamsGroupMember member40 = new StreamsGroupMember.Builder(member1.memberId())
            .build();
        final StreamsGroupMember member41 = new StreamsGroupMember.Builder(member40.memberId())
            .setWarmupTasksPendingRevocation(
                mkTasksPerSubtopology(mkTasks("subtopology-id", 1, 2, 3))
            ).build();
        assertEquals(member1, member40);
        assertEquals(member1.hashCode(), member40.hashCode());
        assertNotEquals(member1, member41);
        assertNotEquals(member1.hashCode(), member41.hashCode());
    }

    @Test
    public void testUpdateWithStreamsGroupMemberMetadataValue() {
        StreamsGroupMemberMetadataValue record = new StreamsGroupMemberMetadataValue()
            .setClientId("client-id")
            .setClientHost("host-id")
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(1000)
            .setTopologyEpoch(3)
            .setProcessId("process-id")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
            .setClientTags(Collections.singletonList(new KeyValue().setKey("client").setValue("tag")));

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals(record.clientId(), member.clientId());
        assertEquals(record.clientHost(), member.clientHost());
        assertEquals(record.instanceId(), member.instanceId());
        assertEquals(record.rackId(), member.rackId());
        assertEquals(record.rebalanceTimeoutMs(), member.rebalanceTimeoutMs());
        assertEquals(record.topologyEpoch(), member.topologyEpoch());
        assertEquals(record.processId(), member.processId());
        assertEquals(record.userEndpoint(), member.userEndpoint());
        assertEquals(
            record.clientTags().stream().collect(Collectors.toMap(KeyValue::key, KeyValue::value)),
            member.clientTags()
        );
        assertEquals("member-id", member.memberId());
        assertEquals(0, member.memberEpoch());
        assertEquals(-1, member.previousMemberEpoch());
        assertEquals(MemberState.STABLE, member.state());
        assertEquals(Collections.emptyMap(), member.assignedActiveTasks());
        assertEquals(Collections.emptyMap(), member.assignedStandbyTasks());
        assertEquals(Collections.emptyMap(), member.assignedWarmupTasks());
        assertEquals(Collections.emptyMap(), member.activeTasksPendingRevocation());
        assertEquals(Collections.emptyMap(), member.standbyTasksPendingRevocation());
        assertEquals(Collections.emptyMap(), member.warmupTasksPendingRevocation());
    }

    @Test
    public void testUpdateWithConsumerGroupCurrentMemberAssignmentValue() {
        final String subtopology1 = "subtopology-id1";
        final String subtopology2 = "subtopology-id2";
        final List<Integer> partitions1 = Arrays.asList(1, 2);
        final List<Integer> partitions2 = Arrays.asList(3, 4);
        final List<Integer> partitions3 = Arrays.asList(5, 6);
        final List<Integer> partitions4 = Arrays.asList(7, 8);
        final List<Integer> partitions5 = Arrays.asList(9, 10);
        final List<Integer> partitions6 = Arrays.asList(11, 12);

        StreamsGroupCurrentMemberAssignmentValue record = new StreamsGroupCurrentMemberAssignmentValue()
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setState((byte) 2)
            .setActiveTasks(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology1)
                .setPartitions(partitions1))
            )
            .setStandbyTasks(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology2)
                .setPartitions(partitions2))
            )
            .setWarmupTasks(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology1)
                .setPartitions(partitions3))
            )
            .setActiveTasksPendingRevocation(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology2)
                .setPartitions(partitions4))
            )
            .setStandbyTasksPendingRevocation(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology1)
                .setPartitions(partitions5))
            )
            .setWarmupTasksPendingRevocation(Collections.singletonList(new TaskIds()
                .setSubtopologyId(subtopology2)
                .setPartitions(partitions6))
            );

        StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertEquals(record.memberEpoch(), member.memberEpoch());
        assertEquals(record.previousMemberEpoch(), member.previousMemberEpoch());
        assertEquals(MemberState.fromValue(record.state()), member.state());
        assertEquals(
            Map.of(subtopology1, new HashSet<>(partitions1)),
            member.assignedActiveTasks()
        );
        assertEquals(
            Map.of(subtopology2, new HashSet<>(partitions2)),
            member.assignedStandbyTasks()
        );
        assertEquals(
            Map.of(subtopology1, new HashSet<>(partitions3)),
            member.assignedWarmupTasks()
        );
        assertEquals(
            Map.of(subtopology2, new HashSet<>(partitions4)),
            member.activeTasksPendingRevocation()
        );
        assertEquals(
            Map.of(subtopology1, new HashSet<>(partitions5)),
            member.standbyTasksPendingRevocation()
        );
        assertEquals(
            Map.of(subtopology2, new HashSet<>(partitions6)),
            member.warmupTasksPendingRevocation()
        );
        assertEquals("member-id", member.memberId());
        assertNull(member.instanceId());
        assertNull(member.rackId());
        assertEquals(-1, member.rebalanceTimeoutMs());
        assertEquals("", member.clientId());
        assertEquals("", member.clientHost());
        assertEquals(-1, member.topologyEpoch());
        assertNull(member.processId());
        assertNull(member.userEndpoint());
        assertEquals(Collections.emptyMap(), member.clientTags());
    }

    @Test
    public void testMaybeUpdateMember() {
        final String subtopology1 = "subtopology-id1";
        final String subtopology2 = "subtopology-id2";

        final StreamsGroupMember member = new StreamsGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("hostname")
            .setTopologyEpoch(3)
            .setProcessId("process-id")
            .setUserEndpoint(new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090))
            .setClientTags(mkMap(mkEntry("client", "tag")))
            .setAssignedActiveTasks(mkTasksPerSubtopology(mkTasks(subtopology1, 1, 2, 3)))
            .setAssignedStandbyTasks(mkTasksPerSubtopology(mkTasks(subtopology2, 6, 5, 4)))
            .setAssignedWarmupTasks(mkTasksPerSubtopology(mkTasks(subtopology1, 7, 8, 9)))
            .setActiveTasksPendingRevocation(
                mkTasksPerSubtopology(mkTasks(subtopology2, 3, 2, 1)))
            .build();

        // This is a no-op.
        StreamsGroupMember updatedMember = new StreamsGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.empty())
            .maybeUpdateInstanceId(Optional.empty())
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.empty())
            .maybeUpdateProcessId(Optional.empty())
            .maybeUpdateTopologyEpoch(OptionalInt.empty())
            .maybeUpdateUserEndpoint(Optional.empty())
            .maybeUpdateClientTags(Optional.empty())
            .build();

        assertEquals(member, updatedMember);

        final String newRackId = "new" + member.rackId();
        final String newInstanceId = "new" + member.instanceId();
        final long newRebalanceTimeout = member.rebalanceTimeoutMs() + 1000;
        final String newProcessId = "new" + member.processId();
        final int newTopologyEpoch = member.topologyEpoch() + 1;
        final StreamsGroupMemberMetadataValue.Endpoint newUserEndpoint =
            new StreamsGroupMemberMetadataValue.Endpoint().setHost(member.userEndpoint().host() + "2").setPort(9090);
        final Map<String, String> newClientTags = new HashMap<>(member.clientTags());
        newClientTags.put("client2", "tag2");

        updatedMember = new StreamsGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.of(newRackId))
            .maybeUpdateInstanceId(Optional.of(newInstanceId))
            .maybeUpdateRebalanceTimeoutMs(OptionalInt.of(6000))
            .maybeUpdateProcessId(Optional.of(newProcessId))
            .maybeUpdateTopologyEpoch(OptionalInt.of(newTopologyEpoch))
            .maybeUpdateUserEndpoint(Optional.of(newUserEndpoint))
            .maybeUpdateClientTags(Optional.of(newClientTags))
            .build();

        assertEquals(newRackId, updatedMember.rackId());
        assertEquals(newInstanceId, updatedMember.instanceId());
        assertEquals(newRebalanceTimeout, updatedMember.rebalanceTimeoutMs());
        assertEquals(newProcessId, updatedMember.processId());
        assertEquals(newTopologyEpoch, updatedMember.topologyEpoch());
        assertEquals(newUserEndpoint, updatedMember.userEndpoint());
        assertEquals(newClientTags, updatedMember.clientTags());
        assertEquals(member.memberId(), updatedMember.memberId());
        assertEquals(member.memberEpoch(), updatedMember.memberEpoch());
        assertEquals(member.previousMemberEpoch(), updatedMember.previousMemberEpoch());
        assertEquals(member.state(), updatedMember.state());
        assertEquals(member.clientId(), updatedMember.clientId());
        assertEquals(member.clientHost(), updatedMember.clientHost());
        assertEquals(member.assignedActiveTasks(), updatedMember.assignedActiveTasks());
        assertEquals(member.assignedStandbyTasks(), updatedMember.assignedStandbyTasks());
        assertEquals(member.assignedWarmupTasks(), updatedMember.assignedWarmupTasks());
        assertEquals(member.activeTasksPendingRevocation(), updatedMember.activeTasksPendingRevocation());
        assertEquals(member.standbyTasksPendingRevocation(), updatedMember.standbyTasksPendingRevocation());
        assertEquals(member.warmupTasksPendingRevocation(), updatedMember.warmupTasksPendingRevocation());
    }

    @Test
    public void testUpdateMemberEpoch() {
        final StreamsGroupMember member = new StreamsGroupMember.Builder("member-id").build();

        final int newMemberEpoch = member.memberEpoch() + 1;
        final StreamsGroupMember updatedMember = new StreamsGroupMember.Builder(member)
            .updateMemberEpoch(newMemberEpoch)
            .build();

        assertEquals(member.memberId(), updatedMember.memberId());
        assertEquals(newMemberEpoch, updatedMember.memberEpoch());
        // The previous member epoch becomes the old current member epoch.
        assertEquals(member.memberEpoch(), updatedMember.previousMemberEpoch());
        assertEquals(member.state(), updatedMember.state());
        assertEquals(member.instanceId(), updatedMember.instanceId());
        assertEquals(member.rackId(), updatedMember.rackId());
        assertEquals(member.rebalanceTimeoutMs(), updatedMember.rebalanceTimeoutMs());
        assertEquals(member.clientId(), updatedMember.clientId());
        assertEquals(member.clientHost(), updatedMember.clientHost());
        assertEquals(member.topologyEpoch(), updatedMember.topologyEpoch());
        assertNull(member.processId());
        assertNull(member.userEndpoint());
        assertEquals(member.clientTags(), updatedMember.clientTags());
        assertEquals(member.assignedActiveTasks(), updatedMember.assignedActiveTasks());
        assertEquals(member.assignedStandbyTasks(), updatedMember.assignedStandbyTasks());
        assertEquals(member.assignedWarmupTasks(), updatedMember.assignedWarmupTasks());
        assertEquals(member.activeTasksPendingRevocation(), updatedMember.activeTasksPendingRevocation());
        assertEquals(member.standbyTasksPendingRevocation(), updatedMember.standbyTasksPendingRevocation());
        assertEquals(member.warmupTasksPendingRevocation(), updatedMember.warmupTasksPendingRevocation());
    }

    @Test
    public void testAsStreamsGroupDescribeMember() {
        String subTopology1 = Uuid.randomUuid().toString();
        String subTopology2 = Uuid.randomUuid().toString();
        String subTopology3 = Uuid.randomUuid().toString();
        List<Integer> assignedTasks1 = Arrays.asList(0, 1, 2);
        List<Integer> assignedTasks2 = Arrays.asList(3, 4, 5);
        List<Integer> assignedTasks3 = Arrays.asList(6, 7, 8);
        int epoch = 10;
        String memberId = Uuid.randomUuid().toString();
        String clientId = "clientId";
        String instanceId = "instanceId";
        String rackId = "rackId";
        String clientHost = "clientHost";
        String processId = "processId";
        int topologyEpoch = 3;
        Map<String, String> clientTags = Collections.singletonMap("key", "value");
        Assignment targetAssignment = new Assignment(
            mkMap(mkEntry(subTopology1, new HashSet<>(assignedTasks3))),
            mkMap(mkEntry(subTopology2, new HashSet<>(assignedTasks2))),
            mkMap(mkEntry(subTopology3, new HashSet<>(assignedTasks1)))
        );
        StreamsGroupMember member = new StreamsGroupMember.Builder(memberId)
            .setMemberEpoch(epoch)
            .setPreviousMemberEpoch(epoch - 1)
            .setClientId(clientId)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setClientHost(clientHost)
            .setProcessId(processId)
            .setTopologyEpoch(topologyEpoch)
            .setClientTags(clientTags)
            .setAssignedActiveTasks(
                mkMap(mkEntry(subTopology1, new HashSet<>(assignedTasks1)))
            )
            .setAssignedStandbyTasks(
                mkMap(mkEntry(subTopology2, new HashSet<>(assignedTasks2)))
            )
            .setAssignedWarmupTasks(
                mkMap(mkEntry(subTopology3, new HashSet<>(assignedTasks3)))
            )
            .setUserEndpoint(
                new StreamsGroupMemberMetadataValue.Endpoint().setHost("host").setPort(9090)
            )
            .build();

        StreamsGroupDescribeResponseData.Member actual = member.asStreamsGroupDescribeMember(targetAssignment);
        StreamsGroupDescribeResponseData.Member expected = new StreamsGroupDescribeResponseData.Member()
            .setMemberId(memberId)
            .setMemberEpoch(epoch)
            .setClientId(clientId)
            .setInstanceId(instanceId)
            .setRackId(rackId)
            .setClientHost(clientHost)
            .setProcessId(processId)
            .setTopologyEpoch(topologyEpoch)
            .setClientTags(Collections.singletonList(new StreamsGroupDescribeResponseData.KeyValue().setKey("key").setValue("value")))
            .setAssignment(
                new StreamsGroupDescribeResponseData.Assignment()
                    .setActiveTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology1)
                        .setPartitions(assignedTasks1)))
                    .setStandbyTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology2)
                        .setPartitions(assignedTasks2)))
                    .setWarmupTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology3)
                        .setPartitions(assignedTasks3)))
            )
            .setTargetAssignment(
                new StreamsGroupDescribeResponseData.Assignment()
                    .setActiveTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology1)
                        .setPartitions(assignedTasks3)))
                    .setStandbyTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology2)
                        .setPartitions(assignedTasks2)))
                    .setWarmupTasks(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskIds()
                        .setSubtopologyId(subTopology3)
                        .setPartitions(assignedTasks1)))
            )
            .setUserEndpoint(new StreamsGroupDescribeResponseData.Endpoint().setHost("host").setPort(9090));
        // TODO: TaskOffset, TaskEndOffset, IsClassic are to be implemented.

        assertEquals(expected, actual);
    }

    @Test
    public void testAsStreamsGroupDescribeWithTargetAssignmentNull() {
        StreamsGroupMember member = new StreamsGroupMember.Builder(Uuid.randomUuid().toString())
            .build();

        StreamsGroupDescribeResponseData.Member streamsGroupDescribeMember = member.asStreamsGroupDescribeMember(
            null);

        assertEquals(new StreamsGroupDescribeResponseData.Assignment(), streamsGroupDescribeMember.targetAssignment());
    }
}
