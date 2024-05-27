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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.coordinator.group.AbstractGroup;
import org.apache.kafka.coordinator.group.GroupMember;
import org.apache.kafka.coordinator.group.CoordinatorRecord;
import org.apache.kafka.coordinator.group.CoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.common.MemberState;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineObject;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState.ASSIGNING;
import static org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState.RECONCILING;
import static org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState.STABLE;

/**
 * A Consumer Group. All the metadata in this class are backed by
 * records in the __consumer_offsets partitions.
 */
public class ConsumerGroup extends AbstractGroup<ConsumerGroupMember> {

    public enum ConsumerGroupState {
        EMPTY("Empty"),
        ASSIGNING("Assigning"),
        RECONCILING("Reconciling"),
        STABLE("Stable"),
        DEAD("Dead");

        private final String name;

        private final String lowerCaseName;

        ConsumerGroupState(String name) {
            this.name = name;
            this.lowerCaseName = name.toLowerCase(Locale.ROOT);
        }

        @Override
        public String toString() {
            return name;
        }

        public String toLowerCaseString() {
            return lowerCaseName;
        }
    }

    /**
     * The group state.
     */
    private final TimelineObject<ConsumerGroupState> state;

    /**
     * The static group members.
     */
    private final TimelineHashMap<String, String> staticMembers;

    /**
     * The number of members supporting each server assignor name.
     */
    private final TimelineHashMap<String, Integer> serverAssignors;

    /**
     * The coordinator metrics.
     */
    private final GroupCoordinatorMetricsShard metrics;

    /**
     * The number of members that use the classic protocol.
     */
    private final TimelineInteger numClassicProtocolMembers;

    /**
     * Map of protocol names to the number of members that use classic protocol and support them.
     */
    private final TimelineHashMap<String, Integer> classicProtocolMembersSupportedProtocols;

    public ConsumerGroup(
        SnapshotRegistry snapshotRegistry,
        String groupId,
        GroupCoordinatorMetricsShard metrics
    ) {
        super(snapshotRegistry, groupId);
        this.state = new TimelineObject<>(snapshotRegistry, EMPTY);
        this.staticMembers = new TimelineHashMap<>(snapshotRegistry, 0);
        this.serverAssignors = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metrics = Objects.requireNonNull(metrics);
        this.numClassicProtocolMembers = new TimelineInteger(snapshotRegistry);
        this.classicProtocolMembersSupportedProtocols = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * @return The group type (Consumer).
     */
    @Override
    public GroupType type() {
        return GroupType.CONSUMER;
    }

    /**
     * @return The current state as a String.
     */
    @Override
    public String stateAsString() {
        return state.get().toString();
    }

    /**
     * @return The current state as a String with given committedOffset.
     */
    public String stateAsString(long committedOffset) {
        return state.get(committedOffset).toString();
    }

    /**
     * @return The current state.
     */
    public ConsumerGroupState state() {
        return state.get();
    }

    /**
     * @return The current state based on committed offset.
     */
    public ConsumerGroupState state(long committedOffset) {
        return state.get(committedOffset);
    }

    /**
     * Sets the number of members using the classic protocol.
     *
     * @param numClassicProtocolMembers The new NumClassicProtocolMembers.
     */
    public void setNumClassicProtocolMembers(int numClassicProtocolMembers) {
        this.numClassicProtocolMembers.set(numClassicProtocolMembers);
    }

    /**
     * Get member id of a static member that matches the given group
     * instance id.
     *
     * @param groupInstanceId The group instance id.
     *
     * @return The member id corresponding to the given instance id or null if it does not exist
     */
    public String staticMemberId(String groupInstanceId) {
        return staticMembers.get(groupInstanceId);
    }

    /**
     * Gets or creates a new member but without adding it to the group. Adding a member
     * is done via the {@link ConsumerGroup#updateMember(ConsumerGroupMember)} method.
     *
     * @param memberId          The member id.
     * @param createIfNotExists Booleans indicating whether the member must be
     *                          created if it does not exist.
     *
     * @return A ConsumerGroupMember.
     */
    public ConsumerGroupMember getOrMaybeCreateMember(
        String memberId,
        boolean createIfNotExists
    ) {
        ConsumerGroupMember member = members.get(memberId);
        if (member != null) return member;

        if (!createIfNotExists) {
            throw new UnknownMemberIdException(
                String.format("Member %s is not a member of group %s.", memberId, groupId)
            );
        }

        return new ConsumerGroupMember.Builder(memberId).build();
    }

    /**
     * Gets a static member.
     *
     * @param instanceId The group instance id.
     *
     * @return The member corresponding to the given instance id or null if it does not exist
     */
    public ConsumerGroupMember staticMember(String instanceId) {
        String existingMemberId = staticMemberId(instanceId);
        return existingMemberId == null ? null : getOrMaybeCreateMember(existingMemberId, false);
    }

    @Override
    public void updateMember(ConsumerGroupMember newMember) {
        if (newMember == null) {
            throw new IllegalArgumentException("newMember cannot be null.");
        }

        ConsumerGroupMember oldMember = members.put(newMember.memberId(), newMember);
        maybeUpdateSubscribedTopicNamesAndGroupSubscriptionType(oldMember, newMember);
        maybeUpdateServerAssignors(oldMember, newMember);
        maybeUpdatePartitionEpoch(oldMember, newMember);
        updateStaticMember(newMember);
        maybeUpdateGroupState();
        maybeUpdateNumClassicProtocolMembers(oldMember, newMember);
        maybeUpdateClassicProtocolMembersSupportedProtocols(oldMember, newMember);
    }

    /**
     * Updates the member id stored against the instance id if the member is a static member.
     *
     * @param newMember The new member state.
     */
    private void updateStaticMember(ConsumerGroupMember newMember) {
        if (newMember.instanceId() != null) {
            staticMembers.put(newMember.instanceId(), newMember.memberId());
        }
    }

    @Override
    public void removeMember(String memberId) {
        ConsumerGroupMember oldMember = members.remove(memberId);
        maybeUpdateSubscribedTopicNamesAndGroupSubscriptionType(oldMember, null);
        maybeUpdateServerAssignors(oldMember, null);
        maybeRemovePartitionEpoch(oldMember);
        removeStaticMember(oldMember);
        maybeUpdateGroupState();
        maybeUpdateNumClassicProtocolMembers(oldMember, null);
        maybeUpdateClassicProtocolMembersSupportedProtocols(oldMember, null);
    }

    /**
     * Remove the static member mapping if the removed member is static.
     *
     * @param oldMember The member to remove.
     */
    private void removeStaticMember(ConsumerGroupMember oldMember) {
        if (oldMember.instanceId() != null) {
            staticMembers.remove(oldMember.instanceId());
        }
    }

    /**
     * @return The number of members that use the classic protocol.
     */
    public int numClassicProtocolMembers() {
        return numClassicProtocolMembers.get();
    }

    /**
     * @return The map of the protocol name and the number of members using the classic protocol that support it.
     */
    public Map<String, Integer> classicMembersSupportedProtocols() {
        return Collections.unmodifiableMap(classicProtocolMembersSupportedProtocols);
    }

    /**
     * @return An immutable Map containing all the static members keyed by instance id.
     */
    public Map<String, String> staticMembers() {
        return Collections.unmodifiableMap(staticMembers);
    }

    /**
     * Compute the preferred (server side) assignor for the group while
     * taking into account the updated member. The computation relies
     * on {{@link ConsumerGroup#serverAssignors}} persisted structure
     * but it does not update it.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     *
     * @return An Optional containing the preferred assignor.
     */
    public Optional<String> computePreferredServerAssignor(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        // Copy the current count and update it.
        Map<String, Integer> counts = new HashMap<>(this.serverAssignors);
        maybeUpdateServerAssignors(counts, oldMember, newMember);

        return counts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
    }

    /**
     * @return The preferred assignor for the group.
     */
    public Optional<String> preferredServerAssignor() {
        return preferredServerAssignor(Long.MAX_VALUE);
    }

    /**
     * @return The preferred assignor for the group with given offset.
     */
    public Optional<String> preferredServerAssignor(long committedOffset) {
        return serverAssignors.entrySet(committedOffset).stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
    }

    /**
     * Validates the DeleteGroups request.
     */
    @Override
    public void validateDeleteGroup() throws ApiException {
        if (state() != ConsumerGroupState.EMPTY) {
            throw Errors.NON_EMPTY_GROUP.exception();
        }
    }

    /**
     * Populates the list of records with tombstone(s) for deleting the group.
     *
     * @param records The list of records.
     */
    @Override
    public void createGroupTombstoneRecords(List<CoordinatorRecord> records) {
        members().forEach((memberId, member) ->
            records.add(CoordinatorRecordHelpers.newCurrentAssignmentTombstoneRecord(groupId(), memberId))
        );

        members().forEach((memberId, member) ->
            records.add(CoordinatorRecordHelpers.newTargetAssignmentTombstoneRecord(groupId(), memberId))
        );
        records.add(CoordinatorRecordHelpers.newTargetAssignmentEpochTombstoneRecord(groupId()));

        members().forEach((memberId, member) ->
            records.add(CoordinatorRecordHelpers.newMemberSubscriptionTombstoneRecord(groupId(), memberId))
        );

        records.add(CoordinatorRecordHelpers.newGroupSubscriptionMetadataTombstoneRecord(groupId()));
        records.add(CoordinatorRecordHelpers.newGroupEpochTombstoneRecord(groupId()));
    }

    @Override
    public boolean isEmpty() {
        return state() == ConsumerGroupState.EMPTY;
    }

    @Override
    public boolean isInStates(Set<String> statesFilter, long committedOffset) {
        return statesFilter.contains(state.get(committedOffset).toLowerCaseString());
    }

    @Override
    protected void maybeUpdateGroupState() {
        ConsumerGroupState previousState = state.get();
        ConsumerGroupState newState = STABLE;
        if (members.isEmpty()) {
            newState = EMPTY;
        } else if (groupEpoch.get() > targetAssignmentEpoch.get()) {
            newState = ASSIGNING;
        } else {
            for (GroupMember member : members.values()) {
                if (!member.isReconciledTo(targetAssignmentEpoch.get())) {
                    newState = RECONCILING;
                    break;
                }
            }
        }

        state.set(newState);
        metrics.onConsumerGroupStateTransition(previousState, newState);
    }

    /**
     * Updates the server assignors count.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateServerAssignors(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        maybeUpdateServerAssignors(serverAssignors, oldMember, newMember);
    }

    /**
     * Updates the server assignors count.
     *
     * @param serverAssignorCount   The count to update.
     * @param oldMember             The old member.
     * @param newMember             The new member.
     */
    private static void maybeUpdateServerAssignors(
        Map<String, Integer> serverAssignorCount,
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.serverAssignorName().ifPresent(name ->
                serverAssignorCount.compute(name, ConsumerGroup::decValue)
            );
        }
        if (newMember != null) {
            newMember.serverAssignorName().ifPresent(name ->
                serverAssignorCount.compute(name, ConsumerGroup::incValue)
            );
        }
    }

    /**
     * Updates the number of the members that use the classic protocol.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateNumClassicProtocolMembers(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        int delta = 0;
        if (oldMember != null && oldMember.useClassicProtocol()) {
            delta--;
        }
        if (newMember != null && newMember.useClassicProtocol()) {
            delta++;
        }
        setNumClassicProtocolMembers(numClassicProtocolMembers() + delta);
    }

    /**
     * Updates the supported protocol count of the members that use the classic protocol.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateClassicProtocolMembersSupportedProtocols(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.supportedClassicProtocols().ifPresent(protocols ->
                protocols.forEach(protocol ->
                    classicProtocolMembersSupportedProtocols.compute(protocol.name(), ConsumerGroup::decValue)
                )
            );
        }
        if (newMember != null) {
            newMember.supportedClassicProtocols().ifPresent(protocols ->
                protocols.forEach(protocol ->
                    classicProtocolMembersSupportedProtocols.compute(protocol.name(), ConsumerGroup::incValue)
                )
            );
        }
    }

    /**
     * Updates the partition epochs based on the old and the new member.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdatePartitionEpoch(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        maybeRemovePartitionEpoch(oldMember);
        addPartitionEpochs(newMember.assignedPartitions(), newMember.memberEpoch());
        addPartitionEpochs(newMember.partitionsPendingRevocation(), newMember.memberEpoch());
    }

    /**
     * Removes the partition epochs for the provided member.
     *
     * @param oldMember The old member.
     */
    private void maybeRemovePartitionEpoch(
        ConsumerGroupMember oldMember
    ) {
        if (oldMember != null) {
            removePartitionEpochs(oldMember.assignedPartitions(), oldMember.memberEpoch());
            removePartitionEpochs(oldMember.partitionsPendingRevocation(), oldMember.memberEpoch());
        }
    }

    /**
     * Removes the partition epochs based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param expectedEpoch The expected epoch.
     * @throws IllegalStateException if the epoch does not match the expected one.
     * package-private for testing.
     */
    void removePartitionEpochs(
        Map<Uuid, Set<Integer>> assignment,
        int expectedEpoch
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull != null) {
                    assignedPartitions.forEach(partitionId -> {
                        Integer prevValue = partitionsOrNull.remove(partitionId);
                        if (prevValue != expectedEpoch) {
                            throw new IllegalStateException(
                                String.format("Cannot remove the epoch %d from %s-%s because the partition is " +
                                    "still owned at a different epoch %d", expectedEpoch, topicId, partitionId, prevValue));
                        }
                    });
                    if (partitionsOrNull.isEmpty()) {
                        return null;
                    } else {
                        return partitionsOrNull;
                    }
                } else {
                    throw new IllegalStateException(
                        String.format("Cannot remove the epoch %d from %s because it does not have any epoch",
                            expectedEpoch, topicId));
                }
            });
        });
    }

    /**
     * Adds the partitions epoch based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param epoch         The new epoch.
     * @throws IllegalStateException if the partition already has an epoch assigned.
     * package-private for testing.
     */
    void addPartitionEpochs(
        Map<Uuid, Set<Integer>> assignment,
        int epoch
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull == null) {
                    partitionsOrNull = new TimelineHashMap<>(snapshotRegistry, assignedPartitions.size());
                }
                for (Integer partitionId : assignedPartitions) {
                    Integer prevValue = partitionsOrNull.put(partitionId, epoch);
                    if (prevValue != null) {
                        throw new IllegalStateException(
                            String.format("Cannot set the epoch of %s-%s to %d because the partition is " +
                                "still owned at epoch %d", topicId, partitionId, epoch, prevValue));
                    }
                }
                return partitionsOrNull;
            });
        });
    }

    public ConsumerGroupDescribeResponseData.DescribedGroup asDescribedGroup(
        long committedOffset,
        String defaultAssignor,
        TopicsImage topicsImage
    ) {
        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setAssignorName(preferredServerAssignor(committedOffset).orElse(defaultAssignor))
            .setGroupEpoch(groupEpoch.get(committedOffset))
            .setGroupState(state.get(committedOffset).toString())
            .setAssignmentEpoch(targetAssignmentEpoch.get(committedOffset));
        members.entrySet(committedOffset).forEach(
            entry -> describedGroup.members().add(
                (entry.getValue()).asConsumerGroupDescribeMember(
                    targetAssignment.get(entry.getValue().memberId(), committedOffset),
                    topicsImage
                )
            )
        );
        return describedGroup;
    }

    /**
     * Create a new consumer group according to the given classic group.
     *
     * @param snapshotRegistry  The SnapshotRegistry.
     * @param metrics           The GroupCoordinatorMetricsShard.
     * @param classicGroup      The converted classic group.
     * @param topicsImage       The TopicsImage for topic id and topic name conversion.
     * @return  The created ConsumerGruop.
     */
    public static ConsumerGroup fromClassicGroup(
        SnapshotRegistry snapshotRegistry,
        GroupCoordinatorMetricsShard metrics,
        ClassicGroup classicGroup,
        TopicsImage topicsImage
    ) {
        String groupId = classicGroup.groupId();
        ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId, metrics);
        consumerGroup.setGroupEpoch(classicGroup.generationId());
        consumerGroup.setTargetAssignmentEpoch(classicGroup.generationId());

        classicGroup.allMembers().forEach(classicGroupMember -> {
            ConsumerPartitionAssignor.Assignment assignment = ConsumerProtocol.deserializeAssignment(
                ByteBuffer.wrap(classicGroupMember.assignment())
            );
            Map<Uuid, Set<Integer>> partitions = topicPartitionMapFromList(assignment.partitions(), topicsImage);

            ConsumerPartitionAssignor.Subscription subscription = ConsumerProtocol.deserializeSubscription(
                ByteBuffer.wrap(classicGroupMember.metadata(classicGroup.protocolName().get()))
            );

            // The target assignment and the assigned partitions of each member are set based on the last
            // assignment of the classic group. All the members are put in the Stable state. If the classic
            // group was in Preparing Rebalance or Completing Rebalance states, the classic members are
            // asked to rejoin the group to re-trigger a rebalance or collect their assignments.
            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(classicGroupMember.memberId())
                .setMemberEpoch(classicGroup.generationId())
                .setState(MemberState.STABLE)
                .setPreviousMemberEpoch(classicGroup.generationId())
                .setInstanceId(classicGroupMember.groupInstanceId().orElse(null))
                .setRackId(subscription.rackId().orElse(null))
                .setRebalanceTimeoutMs(classicGroupMember.rebalanceTimeoutMs())
                .setClientId(classicGroupMember.clientId())
                .setClientHost(classicGroupMember.clientHost())
                .setSubscribedTopicNames(subscription.topics())
                .setAssignedPartitions(partitions)
                .setClassicMemberMetadata(
                    new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                        .setSessionTimeoutMs(classicGroupMember.sessionTimeoutMs())
                        .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                            classicGroupMember.supportedProtocols()
                        ))
                )
                .build();
            consumerGroup.updateTargetAssignment(newMember.memberId(), new Assignment(partitions));
            consumerGroup.updateMember(newMember);
        });

        return consumerGroup;
    }

    /**
     * Populate the record list with the records needed to create the given consumer group.
     *
     * @param records The list to which the new records are added.
     */
    public void createConsumerGroupRecords(
        List<CoordinatorRecord> records
    ) {
        members().forEach((__, consumerGroupMember) ->
            records.add(CoordinatorRecordHelpers.newMemberSubscriptionRecord(groupId(), consumerGroupMember))
        );

        records.add(CoordinatorRecordHelpers.newGroupEpochRecord(groupId(), groupEpoch()));

        members().forEach((consumerGroupMemberId, consumerGroupMember) ->
            records.add(CoordinatorRecordHelpers.newTargetAssignmentRecord(
                groupId(),
                consumerGroupMemberId,
                targetAssignment(consumerGroupMemberId).partitions()
            ))
        );

        records.add(CoordinatorRecordHelpers.newTargetAssignmentEpochRecord(groupId(), groupEpoch()));

        members().forEach((__, consumerGroupMember) ->
            records.add(CoordinatorRecordHelpers.newCurrentAssignmentRecord(groupId(), consumerGroupMember))
        );
    }

    /**
     * @return The map of topic id and partition set converted from the list of TopicPartition.
     */
    private static Map<Uuid, Set<Integer>> topicPartitionMapFromList(
        List<TopicPartition> partitions,
        TopicsImage topicsImage
    ) {
        Map<Uuid, Set<Integer>> topicPartitionMap = new HashMap<>();
        partitions.forEach(topicPartition -> {
            TopicImage topicImage = topicsImage.getTopic(topicPartition.topic());
            if (topicImage != null) {
                topicPartitionMap
                    .computeIfAbsent(topicImage.id(), __ -> new HashSet<>())
                    .add(topicPartition.partition());
            }
        });
        return topicPartitionMap;
    }

    /**
     * Checks whether at least one of the given protocols can be supported. A
     * protocol can be supported if it is supported by all members that use the
     * classic protocol.
     *
     * @param memberProtocolType  The member protocol type.
     * @param memberProtocols     The set of protocol names.
     *
     * @return A boolean based on the condition mentioned above.
     */
    public boolean supportsClassicProtocols(String memberProtocolType, Set<String> memberProtocols) {
        if (ConsumerProtocol.PROTOCOL_TYPE.equals(memberProtocolType)) {
            if (isEmpty()) {
                return !memberProtocols.isEmpty();
            } else {
                return memberProtocols.stream().anyMatch(
                    name -> classicProtocolMembersSupportedProtocols.getOrDefault(name, 0) == numClassicProtocolMembers()
                );
            }
        }
        return false;
    }

    /**
     * Checks whether all the members use the classic protocol except the given member.
     *
     * @param memberId The member to remove.
     * @return A boolean indicating whether all the members use the classic protocol.
     */
    public boolean allMembersUseClassicProtocolExcept(String memberId) {
        return numClassicProtocolMembers() == members().size() - 1 &&
            !getOrMaybeCreateMember(memberId, false).useClassicProtocol();
    }

    /**
     * Checks whether the member has any unreleased partition.
     *
     * @param member The member to check.
     * @return A boolean indicating whether the member has partitions in the target
     *         assignment that hasn't been revoked by other members.
     */
    public boolean waitingOnUnreleasedPartition(ConsumerGroupMember member) {
        if (member.state() == MemberState.UNRELEASED_PARTITIONS) {
            for (Map.Entry<Uuid, Set<Integer>> entry : targetAssignment().get(member.memberId()).partitions().entrySet()) {
                Uuid topicId = entry.getKey();
                Set<Integer> assignedPartitions = member.assignedPartitions().getOrDefault(topicId, Collections.emptySet());

                for (int partition : entry.getValue()) {
                    if (!assignedPartitions.contains(partition) && currentPartitionEpoch(topicId, partition) != -1) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
