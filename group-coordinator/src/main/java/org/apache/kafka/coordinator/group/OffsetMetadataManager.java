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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generic.GenericGroup;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.apache.kafka.coordinator.group.generic.GenericGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.DEAD;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.EMPTY;

/**
 * The OffsetMetadataManager manages the offsets of all the groups. It basically maintains
 * a mapping from group id to topic-partition to offset. This class has two kinds of methods:
 * 1) The request handlers which handle the requests and generate a response and records to
 *    mutate the hard state. Those records will be written by the runtime and applied to the
 *    hard state via the replay methods.
 * 2) The replay methods which apply records to the hard state. Those are used in the request
 *    handling as well as during the initial loading of the records from the partitions.
 */
public class OffsetMetadataManager {
    public static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private Time time = null;
        private GroupMetadataManager groupMetadataManager = null;
        private int offsetMetadataMaxSize = 4096;
        private MetadataImage metadataImage = null;

        Builder withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        Builder withGroupMetadataManager(GroupMetadataManager groupMetadataManager) {
            this.groupMetadataManager = groupMetadataManager;
            return this;
        }

        Builder withOffsetMetadataMaxSize(int offsetMetadataMaxSize) {
            this.offsetMetadataMaxSize = offsetMetadataMaxSize;
            return this;
        }

        Builder withMetadataImage(MetadataImage metadataImage) {
            this.metadataImage = metadataImage;
            return this;
        }

        public OffsetMetadataManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
            if (time == null) time = Time.SYSTEM;

            if (groupMetadataManager == null) {
                throw new IllegalArgumentException("GroupMetadataManager cannot be null");
            }

            return new OffsetMetadataManager(
                snapshotRegistry,
                logContext,
                time,
                metadataImage,
                groupMetadataManager,
                offsetMetadataMaxSize
            );
        }
    }

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The snapshot registry.
     */
    private final SnapshotRegistry snapshotRegistry;

    /**
     * The system time.
     */
    private final Time time;

    /**
     * The metadata image.
     */
    private MetadataImage metadataImage;

    /**
     * The group metadata manager.
     */
    private final GroupMetadataManager groupMetadataManager;

    /**
     * The maximum allowed metadata for any offset commit.
     */
    private final int offsetMetadataMaxSize;

    /**
     * The offsets keyed by topic-partition and group id.
     */
    private final TimelineHashMap<String, TimelineHashMap<TopicPartition, OffsetAndMetadata>> offsetsByGroup;

    OffsetMetadataManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        Time time,
        MetadataImage metadataImage,
        GroupMetadataManager groupMetadataManager,
        int offsetMetadataMaxSize
    ) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(OffsetMetadataManager.class);
        this.time = time;
        this.metadataImage = metadataImage;
        this.groupMetadataManager = groupMetadataManager;
        this.offsetMetadataMaxSize = offsetMetadataMaxSize;
        this.offsetsByGroup = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * Validates an OffsetCommit request.
     *
     * @param context The request context.
     * @param request The actual request.
     */
    private void validateOffsetCommit(
        RequestContext context,
        OffsetCommitRequestData request
    ) throws ApiException {
        Group group;
        try {
            // If the group does not exist and generation id is -1, the request comes from
            // either the admin client or a consumer which does not use the group management
            // facility. In this case, a so-called simple group is created and the request
            // is accepted.
            group = groupMetadataManager.getOrMaybeCreateSimpleGroup(
                request.groupId(),
                request.generationIdOrMemberEpoch() < 0
            );
        } catch (GroupIdNotFoundException ex) {
            // Maintain backward compatibility. This is a bit weird in the
            // context of the new protocol though.
            throw Errors.ILLEGAL_GENERATION.exception();
        }

        // Validate the request based on the group type.
        switch (group.type()) {
            case GENERIC:
                validateOffsetCommitForGenericGroup(
                    (GenericGroup) group,
                    request
                );
                break;

            case CONSUMER:
                validateOffsetCommitForConsumerGroup(
                    (ConsumerGroup) group,
                    context,
                    request
                );
                break;
        }
    }

    /**
     * Validates an OffsetCommit request for a generic group.
     *
     * @param group     The generic group.
     * @param request   The actual request.
     */
    public void validateOffsetCommitForGenericGroup(
        GenericGroup group,
        OffsetCommitRequestData request
    ) throws KafkaException {
        if (group.isInState(DEAD)) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }

        if (request.generationIdOrMemberEpoch() < 0 && group.isInState(EMPTY)) {
            // When the generation id is -1, the request comes from either the admin client
            // or a consumer which does not use the group management facility. In this case,
            // the request can commit offsets if the group is empty.
            return;
        }

        Optional<String> groupInstanceId = OffsetCommitRequest.groupInstanceId(request);
        if (request.generationIdOrMemberEpoch() >= 0 || !request.memberId().isEmpty() || groupInstanceId.isPresent()) {
            // We are validating three things:
            // 1. If the `groupInstanceId` is present, then it exists and is mapped to `memberId`;
            // 2. The `memberId` exists in the group; and
            // 3. The `generationId` matches the current generation id.
            if (groupInstanceId.isPresent()) {
                String memberId = group.staticMemberId(groupInstanceId.get());
                if (memberId == null) {
                    throw Errors.UNKNOWN_MEMBER_ID.exception();
                } else if (!request.memberId().equals(memberId)) {
                    throw Errors.FENCED_INSTANCE_ID.exception();
                }
            }

            if (!group.hasMemberId(request.memberId())) {
                throw Errors.UNKNOWN_MEMBER_ID.exception();
            }

            if (request.generationIdOrMemberEpoch() != group.generationId()) {
                throw Errors.ILLEGAL_GENERATION.exception();
            }
        } else if (!group.isInState(EMPTY)) {
            // If the request does not contain the member id and the generation
            // id (version 0), offset commits are only accepted when the group
            // is not empty.
            throw Errors.UNKNOWN_MEMBER_ID.exception();
        }

        if (group.isInState(COMPLETING_REBALANCE)) {
            // We should not receive a commit request if the group has not completed rebalance;
            // but since the consumer's member.id and generation is valid, it means it has received
            // the latest group generation information from the JoinResponse.
            // So let's return a REBALANCE_IN_PROGRESS to let consumer handle it gracefully.
            throw Errors.REBALANCE_IN_PROGRESS.exception();
        }
    }

    /**
     * Validates an OffsetCommit request for a consumer group.
     *
     * @param group     The consumer group.
     * @param context   The request context.
     * @param request   The actual request.
     */
    public void validateOffsetCommitForConsumerGroup(
        ConsumerGroup group,
        RequestContext context,
        OffsetCommitRequestData request
    ) throws KafkaException {
        if (request.generationIdOrMemberEpoch() < 0 && group.members().isEmpty()) {
            // When the generation id is -1, the request comes from either the admin client
            // or a consumer which does not use the group management facility. In this case,
            // the request can commit offsets if the group is empty.
            return;
        }

        if (!group.hasMember(request.memberId())) {
            throw Errors.UNKNOWN_MEMBER_ID.exception();
        }

        final int memberEpoch = group.getOrMaybeCreateMember(request.memberId(), false).memberEpoch();
        if (request.generationIdOrMemberEpoch() != memberEpoch) {
            // Consumers using the new consumer group protocol (KIP-848) should be using the
            // OffsetCommit API >= 9. As we don't support upgrading from the old to the new
            // protocol yet, we return an UNSUPPORTED_VERSION error if an older version is
            // used. We will revise this when the upgrade path is implemented.
            if (context.header.apiVersion() >= 9) {
                throw Errors.STALE_MEMBER_EPOCH.exception();
            } else {
                throw Errors.UNSUPPORTED_VERSION.exception();
            }
        }
    }

    /**
     * Handles an OffsetCommit request.
     *
     * @param context The request context.
     * @param request The OffsetCommit request.
     *
     * @return A Result containing the OffsetCommitResponseData response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<OffsetCommitResponseData, Record> commitOffset(
        RequestContext context,
        OffsetCommitRequestData request
    ) throws ApiException {
        validateOffsetCommit(context, request);

        final long currentTimeMs = time.milliseconds();
        // "default" expiration timestamp is defined as now + retention. The retention may be overridden
        // in versions from v2 to v4. Otherwise, the retention defined on the broker is used. If an explicit
        // commit timestamp is provided (v1 only), the expiration timestamp is computed based on that.
        final OptionalLong expireTimestampMs = request.retentionTimeMs() == OffsetCommitRequest.DEFAULT_RETENTION_TIME ?
            OptionalLong.empty() : OptionalLong.of(currentTimeMs + request.retentionTimeMs());
        final OffsetCommitResponseData response = new OffsetCommitResponseData();
        final List<Record> records = new ArrayList<>();

        request.topics().forEach(topic -> {
            final OffsetCommitResponseData.OffsetCommitResponseTopic topicResponse =
                new OffsetCommitResponseData.OffsetCommitResponseTopic()
                    .setName(topic.name());
            response.topics().add(topicResponse);

            topic.partitions().forEach(partition -> {
                if (partition.committedMetadata() != null && partition.committedMetadata().length() > offsetMetadataMaxSize) {
                    topicResponse.partitions().add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(Errors.OFFSET_METADATA_TOO_LARGE.code()));
                } else {
                    log.debug("[GroupId {}] Committing offsets {} for partition {}-{} from member {} with leader epoch {}.",
                        request.groupId(), partition.committedOffset(), topic.name(), partition.partitionIndex(),
                        request.memberId(), partition.committedLeaderEpoch());

                    topicResponse.partitions().add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(Errors.NONE.code()));

                    final OptionalInt leaderEpoch = partition.committedLeaderEpoch() == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                        OptionalInt.empty() : OptionalInt.of(partition.committedLeaderEpoch());
                    final String metadata = partition.committedMetadata() == null ?
                        OffsetAndMetadata.NO_METADATA : partition.committedMetadata();
                    final long commitTimestampMs = partition.commitTimestamp() == OffsetCommitRequest.DEFAULT_TIMESTAMP ?
                        currentTimeMs : partition.commitTimestamp();

                    final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(
                        partition.committedOffset(),
                        leaderEpoch,
                        metadata,
                        commitTimestampMs,
                        expireTimestampMs
                    );

                    records.add(RecordHelpers.newOffsetCommitRecord(
                        request.groupId(),
                        topic.name(),
                        partition.partitionIndex(),
                        offsetAndMetadata,
                        metadataImage.features().metadataVersion()
                    ));
                }
            });
        });

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Replays OffsetCommitKey/Value to update or delete the corresponding offsets.
     *
     * @param key   A OffsetCommitKey key.
     * @param value A OffsetCommitValue value.
     */
    public void replay(
        OffsetCommitKey key,
        OffsetCommitValue value
    ) {
        final String groupId = key.group();
        final TopicPartition tp = new TopicPartition(key.topic(), key.partition());

        if (value != null) {
            // Ensures that there is a corresponding group.
            groupMetadataManager.getOrMaybeCreateSimpleGroup(groupId, true);

            final OffsetAndMetadata offsetAndMetadata = OffsetAndMetadata.fromRecord(value);
            TimelineHashMap<TopicPartition, OffsetAndMetadata> offsets = offsetsByGroup.get(groupId);
            if (offsets == null) {
                offsets = new TimelineHashMap<>(snapshotRegistry, 0);
                offsetsByGroup.put(groupId, offsets);
            }

            offsets.put(tp, offsetAndMetadata);
        } else {
            TimelineHashMap<TopicPartition, OffsetAndMetadata> offsets = offsetsByGroup.get(groupId);
            if (offsets != null) {
                offsets.remove(tp);
                if (offsets.isEmpty()) {
                    offsetsByGroup.remove(groupId);
                }
            }
        }
    }

    /**
     * A new metadata image is available.
     *
     * @param newImage  The new metadata image.
     * @param delta     The delta image.
     */
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        metadataImage = newImage;
    }

    /**
     * @return The offset for the provided groupId and topic partition or null
     * if it does not exist.
     *
     * package-private for testing.
     */
    OffsetAndMetadata offset(String groupId, TopicPartition tp) {
        Map<TopicPartition, OffsetAndMetadata> offsets = offsetsByGroup.get(groupId);
        if (offsets == null) {
            return null;
        } else {
            return offsets.get(tp);
        }
    }
}
