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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilder;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetricsShard;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.config.ShareCoordinatorConfig;
import org.apache.kafka.server.group.share.PartitionFactory;
import org.apache.kafka.server.group.share.SharePartitionKey;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ShareCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {
    private final Logger log;
    private final Time time;
    private final CoordinatorTimer<Void, CoordinatorRecord> timer;
    private final ShareCoordinatorConfig config;
    private final CoordinatorMetrics coordinatorMetrics;
    private final CoordinatorMetricsShard metricsShard;
    private final TimelineHashMap<SharePartitionKey, ShareGroupOffset> shareStateMap;  // coord key -> ShareGroupOffset
    private final TimelineHashMap<SharePartitionKey, Integer> leaderEpochMap;
    private final TimelineHashMap<SharePartitionKey, Integer> snapshotUpdateCount;
    private final TimelineHashMap<SharePartitionKey, Integer> stateEpochMap;
    private MetadataImage metadataImage;
    private final int snapshotUpdateRecordsPerSnapshot;

    public static class Builder implements CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> {
        private ShareCoordinatorConfig config;
        private LogContext logContext;
        private SnapshotRegistry snapshotRegistry;
        private Time time;
        private CoordinatorTimer<Void, CoordinatorRecord> timer;
        private CoordinatorMetrics coordinatorMetrics;
        private TopicPartition topicPartition;

        public Builder(ShareCoordinatorConfig config) {
            this.config = config;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withTime(Time time) {
            this.time = time;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withTimer(CoordinatorTimer<Void, CoordinatorRecord> timer) {
            this.timer = timer;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withCoordinatorMetrics(CoordinatorMetrics coordinatorMetrics) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<ShareCoordinatorShard, CoordinatorRecord> withTopicPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
            return this;
        }

        @Override
        @SuppressWarnings("NPathComplexity")
        public ShareCoordinatorShard build() {
            if (logContext == null) logContext = new LogContext();
            if (config == null)
                throw new IllegalArgumentException("Config must be set.");
            if (snapshotRegistry == null)
                throw new IllegalArgumentException("SnapshotRegistry must be set.");
            if (time == null)
                throw new IllegalArgumentException("Time must be set.");
            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");
            if (coordinatorMetrics == null || !(coordinatorMetrics instanceof ShareCoordinatorMetrics))
                throw new IllegalArgumentException("CoordinatorMetrics must be set and be of type ShareCoordinatorMetrics.");
            if (topicPartition == null)
                throw new IllegalArgumentException("TopicPartition must be set.");
            if (config.shareCoordinatorSnapshotUpdateRecordsPerSnapshot() < 0 || config.shareCoordinatorSnapshotUpdateRecordsPerSnapshot() > 500)
                throw new IllegalArgumentException("SnapshotUpdateRecordsPerSnapshot must be between 0 and 500.");

            ShareCoordinatorMetricsShard metricsShard = ((ShareCoordinatorMetrics) coordinatorMetrics)
                .newMetricsShard(snapshotRegistry, topicPartition);

            return new ShareCoordinatorShard(
                logContext,
                time,
                timer,
                config,
                coordinatorMetrics,
                metricsShard,
                snapshotRegistry
            );
        }
    }

    ShareCoordinatorShard(
        LogContext logContext,
        Time time,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        ShareCoordinatorConfig config,
        CoordinatorMetrics coordinatorMetrics,
        CoordinatorMetricsShard metricsShard,
        SnapshotRegistry snapshotRegistry
    ) {
        this.log = logContext.logger(ShareCoordinatorShard.class);
        this.time = time;
        this.timer = timer;
        this.config = config;
        this.coordinatorMetrics = coordinatorMetrics;
        this.metricsShard = metricsShard;
        this.shareStateMap = new TimelineHashMap<>(snapshotRegistry, 0);
        this.leaderEpochMap = new TimelineHashMap<>(snapshotRegistry, 0);
        this.snapshotUpdateCount = new TimelineHashMap<>(snapshotRegistry, 0);
        this.stateEpochMap = new TimelineHashMap<>(snapshotRegistry, 0);
        this.snapshotUpdateRecordsPerSnapshot = config.shareCoordinatorSnapshotUpdateRecordsPerSnapshot();
    }

    @Override
    public void onLoaded(MetadataImage newImage) {
        coordinatorMetrics.activateMetricsShard(metricsShard);
    }

    @Override
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        this.metadataImage = newImage;
    }

    @Override
    public void onUnloaded() {
        coordinatorMetrics.deactivateMetricsShard(metricsShard);
    }

    @Override
    public void replay(long offset, long producerId, short producerEpoch, CoordinatorRecord record) throws RuntimeException {
        ApiMessageAndVersion key = record.key();
        ApiMessageAndVersion value = record.value();

        switch (key.version()) {
            case ShareCoordinator.SHARE_SNAPSHOT_RECORD_KEY_VERSION: // ShareSnapshot
                handleShareSnapshot((ShareSnapshotKey) key.message(), (ShareSnapshotValue) messageOrNull(value));
                break;
            case ShareCoordinator.SHARE_UPDATE_RECORD_KEY_VERSION: // ShareUpdate
                handleShareUpdate((ShareUpdateKey) key.message(), (ShareUpdateValue) messageOrNull(value));
                break;
            default:
                // noop
        }
    }

    private void handleShareSnapshot(ShareSnapshotKey key, ShareSnapshotValue value) {
        SharePartitionKey mapKey = SharePartitionKey.getInstance(key.groupId(), key.topicId(), key.partition());
        maybeUpdateLeaderEpochMap(mapKey, value.leaderEpoch());
        maybeUpdateStateEpochMap(mapKey, value.stateEpoch());

        ShareGroupOffset offsetRecord = ShareGroupOffset.fromRecord(value);
        // this record is the complete snapshot
        shareStateMap.put(mapKey, offsetRecord);
        // if number of share updates is exceeded, then reset it
        snapshotUpdateCount.computeIfPresent(mapKey,
            (k, v) -> v >= config.shareCoordinatorSnapshotUpdateRecordsPerSnapshot() ? 0 : v);
    }

    private void handleShareUpdate(ShareUpdateKey key, ShareUpdateValue value) {
        SharePartitionKey mapKey = SharePartitionKey.getInstance(key.groupId(), key.topicId(), key.partition());
        maybeUpdateLeaderEpochMap(mapKey, value.leaderEpoch());

        // share update does not hold state epoch information.

        ShareGroupOffset offsetRecord = ShareGroupOffset.fromRecord(value);
        // this is an incremental snapshot
        // so, we need to apply it to our current soft state
        shareStateMap.compute(mapKey, (k, v) -> v == null ? offsetRecord : merge(v, value));
        snapshotUpdateCount.compute(mapKey, (k, v) -> v == null ? 0 : v + 1);
    }

    private void maybeUpdateLeaderEpochMap(SharePartitionKey mapKey, int leaderEpoch) {
        leaderEpochMap.putIfAbsent(mapKey, leaderEpoch);
        if (leaderEpochMap.get(mapKey) < leaderEpoch) {
            leaderEpochMap.put(mapKey, leaderEpoch);
        }
    }

    private void maybeUpdateStateEpochMap(SharePartitionKey mapKey, int stateEpoch) {
        stateEpochMap.putIfAbsent(mapKey, stateEpoch);
        if (stateEpochMap.get(mapKey) < stateEpoch) {
            stateEpochMap.put(mapKey, stateEpoch);
        }
    }

    @Override
    public void replayEndTransactionMarker(long producerId, short producerEpoch, TransactionResult result) throws RuntimeException {
        CoordinatorShard.super.replayEndTransactionMarker(producerId, producerEpoch, result);
    }

    /**
     * This method generates the ShareSnapshotValue record corresponding to the requested topic partition information.
     * The generated record is then written to the __share_group_state topic and replayed to the in-memory state
     * of the coordinator shard, shareStateMap, by CoordinatorRuntime.
     * <p>
     * This method as called by the ShareCoordinatorService will be provided with
     * the request data which covers only a single key i.e. group1:topic1:partition1. The implementation
     * below was done keeping this in mind.
     *
     * @param context - RequestContext
     * @param request - WriteShareGroupStateRequestData for a single key
     * @return CoordinatorResult(records, response)
     */
    @SuppressWarnings("NPathComplexity")
    public CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> writeState(
        RequestContext context,
        WriteShareGroupStateRequestData request
    ) {
        log.debug("Write request dump - {}", request);
        // records to write (with both key and value of snapshot type), response to caller
        // only one key will be there in the request by design

        metricsShard.record(ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME);
        Optional<CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord>> error = maybeGetWriteStateError(request);
        if (error.isPresent()) {
            return error.get();
        }

        String groupId = request.groupId();
        WriteShareGroupStateRequestData.WriteStateData topicData = request.topics().get(0);
        WriteShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        SharePartitionKey key = SharePartitionKey.getInstance(groupId, topicData.topicId(), partitionData.partition());
        List<CoordinatorRecord> recordList;

        if (!shareStateMap.containsKey(key)) {
            // since this is the first time we are getting a write request, we should be creating a share snapshot record
            recordList = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
                groupId, topicData.topicId(), partitionData.partition(), ShareGroupOffset.fromRequest(partitionData)
            ));
        } else if (snapshotUpdateCount.getOrDefault(key, 0) >= snapshotUpdateRecordsPerSnapshot) {
            // Since the number of update records for this share part key exceeds snapshotUpdateRecordsPerSnapshot,
            // we should be creating a share snapshot record.
            List<PersisterOffsetsStateBatch> batchesToAdd;
            if (partitionData.startOffset() == -1) {
                batchesToAdd = combineStateBatches(
                    shareStateMap.get(key).stateBatchAsSet(),
                    partitionData.stateBatches().stream()
                        .map(PersisterOffsetsStateBatch::from)
                        .collect(Collectors.toCollection(LinkedHashSet::new)));
            } else {
                // start offset is being updated - we should only
                // consider new updates to batches
                batchesToAdd = partitionData.stateBatches().stream()
                    .map(PersisterOffsetsStateBatch::from).collect(Collectors.toList());
            }

            int newLeaderEpoch = partitionData.leaderEpoch() == -1 ? shareStateMap.get(key).leaderEpoch() : partitionData.leaderEpoch();
            int newStateEpoch = partitionData.stateEpoch() == -1 ? shareStateMap.get(key).stateEpoch() : partitionData.stateEpoch();
            long newStartOffset = partitionData.startOffset() == -1 ? shareStateMap.get(key).startOffset() : partitionData.startOffset();

            recordList = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotRecord(
                groupId, topicData.topicId(), partitionData.partition(),
                new ShareGroupOffset.Builder()
                    .setStartOffset(newStartOffset)
                    .setLeaderEpoch(newLeaderEpoch)
                    .setStateEpoch(newStateEpoch)
                    .setStateBatches(batchesToAdd)
                    .build()));
        } else {
            // share snapshot is present and number of share snapshot update records < snapshotUpdateRecordsPerSnapshot
            recordList = Collections.singletonList(ShareCoordinatorRecordHelpers.newShareSnapshotUpdateRecord(
                groupId, topicData.topicId(), partitionData.partition(), ShareGroupOffset.fromRequest(partitionData, shareStateMap.get(key).snapshotEpoch())
            ));
        }

        List<CoordinatorRecord> validRecords = new LinkedList<>();

        WriteShareGroupStateResponseData responseData = new WriteShareGroupStateResponseData();
        for (CoordinatorRecord record : recordList) {  // should be single record
            if (!(record.key().message() instanceof ShareSnapshotKey) && !(record.key().message() instanceof ShareUpdateKey)) {
                continue;
            }
            SharePartitionKey mapKey = null;
            boolean shouldIncSnapshotEpoch = false;
            if (record.key().message() instanceof ShareSnapshotKey) {
                ShareSnapshotKey recordKey = (ShareSnapshotKey) record.key().message();
                responseData.setResults(Collections.singletonList(WriteShareGroupStateResponse.toResponseWriteStateResult(
                    recordKey.topicId(), Collections.singletonList(WriteShareGroupStateResponse.toResponsePartitionResult(
                        recordKey.partition())))));
                mapKey = SharePartitionKey.getInstance(recordKey.groupId(), recordKey.topicId(), recordKey.partition());
                shouldIncSnapshotEpoch = true;
            } else if (record.key().message() instanceof ShareUpdateKey) {
                ShareUpdateKey recordKey = (ShareUpdateKey) record.key().message();
                responseData.setResults(Collections.singletonList(WriteShareGroupStateResponse.toResponseWriteStateResult(
                    recordKey.topicId(), Collections.singletonList(WriteShareGroupStateResponse.toResponsePartitionResult(
                        recordKey.partition())))));
                mapKey = SharePartitionKey.getInstance(recordKey.groupId(), recordKey.topicId(), recordKey.partition());
            }

            if (shareStateMap.containsKey(mapKey) && shouldIncSnapshotEpoch) {
                ShareGroupOffset oldValue = shareStateMap.get(mapKey);
                ((ShareSnapshotValue) record.value().message()).setSnapshotEpoch(oldValue.snapshotEpoch() + 1);  // increment the snapshot epoch
            }
            validRecords.add(record); // this will have updated snapshot epoch and on replay the value will trickle down to the map
        }

        return new CoordinatorResult<>(validRecords, responseData);
    }

    /**
     * This method finds the ShareSnapshotValue record corresponding to the requested topic partition from the
     * in-memory state of coordinator shard, the shareStateMap.
     * <p>
     * This method as called by the ShareCoordinatorService will be provided with
     * the request data which covers only key i.e. group1:topic1:partition1. The implementation
     * below was done keeping this in mind.
     *
     * @param request - WriteShareGroupStateRequestData for a single key
     * @param offset  - offset to read from the __share_group_state topic partition
     * @return CoordinatorResult(records, response)
     */
    public ReadShareGroupStateResponseData readState(ReadShareGroupStateRequestData request, Long offset) {
        log.debug("Read request dump - {}", request);
        // records to read (with the key of snapshot type), response to caller
        // only one key will be there in the request by design
        Optional<ReadShareGroupStateResponseData> error = maybeGetReadStateError(request, offset);
        if (error.isPresent()) {
            return error.get();
        }

        Uuid topicId = request.topics().get(0).topicId();
        int partition = request.topics().get(0).partitions().get(0).partition();
        int leaderEpoch = request.topics().get(0).partitions().get(0).leaderEpoch();

        SharePartitionKey coordinatorKey = SharePartitionKey.getInstance(request.groupId(), topicId, partition);

        if (!shareStateMap.containsKey(coordinatorKey)) {
            return ReadShareGroupStateResponse.toResponseData(
                topicId,
                partition,
                PartitionFactory.DEFAULT_START_OFFSET,
                PartitionFactory.DEFAULT_STATE_EPOCH,
                Collections.emptyList()
            );
        }

        ShareGroupOffset offsetValue = shareStateMap.get(coordinatorKey, offset);

        if (offsetValue == null) {
            // Returning an error response as the snapshot value was not found
            return ReadShareGroupStateResponse.toErrorResponseData(
                topicId,
                partition,
                Errors.UNKNOWN_SERVER_ERROR,
                "Data not found for topic {}, partition {} for group {}, in the in-memory state of share coordinator"
            );
        }

        List<ReadShareGroupStateResponseData.StateBatch> stateBatches = (offsetValue.stateBatches() != null && !offsetValue.stateBatches().isEmpty()) ?
            offsetValue.stateBatches().stream().map(
                stateBatch -> new ReadShareGroupStateResponseData.StateBatch()
                    .setFirstOffset(stateBatch.firstOffset())
                    .setLastOffset(stateBatch.lastOffset())
                    .setDeliveryState(stateBatch.deliveryState())
                    .setDeliveryCount(stateBatch.deliveryCount())
            ).collect(java.util.stream.Collectors.toList()) : Collections.emptyList();

        // Updating the leader map with the new leader epoch
        leaderEpochMap.put(coordinatorKey, leaderEpoch);

        // Returning the successfully retrieved snapshot value
        return ReadShareGroupStateResponse.toResponseData(topicId, partition, offsetValue.startOffset(), offsetValue.stateEpoch(), stateBatches);
    }

    private Optional<CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord>> maybeGetWriteStateError(
        WriteShareGroupStateRequestData request
    ) {
        String groupId = request.groupId();
        WriteShareGroupStateRequestData.WriteStateData topicData = request.topics().get(0);
        WriteShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();

        if (topicId == null || partitionId < 0) {
            return Optional.of(getWriteErrorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicId, partitionId));
        }

        SharePartitionKey mapKey = SharePartitionKey.getInstance(groupId, topicId, partitionId);
        if (leaderEpochMap.containsKey(mapKey) && leaderEpochMap.get(mapKey) > partitionData.leaderEpoch()) {
            return Optional.of(getWriteErrorResponse(Errors.FENCED_LEADER_EPOCH, topicId, partitionId));
        }
        if (stateEpochMap.containsKey(mapKey) && stateEpochMap.get(mapKey) > partitionData.stateEpoch()) {
            return Optional.of(getWriteErrorResponse(Errors.FENCED_STATE_EPOCH, topicId, partitionId));
        }
        if (metadataImage != null && (metadataImage.topics().getTopic(topicId) == null ||
            metadataImage.topics().getPartition(topicId, partitionId) == null)) {
            return Optional.of(getWriteErrorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicId, partitionId));
        }

        return Optional.empty();
    }

    private Optional<ReadShareGroupStateResponseData> maybeGetReadStateError(ReadShareGroupStateRequestData request, Long offset) {
        String groupId = request.groupId();
        ReadShareGroupStateRequestData.ReadStateData topicData = request.topics().get(0);
        ReadShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

        Uuid topicId = topicData.topicId();
        int partitionId = partitionData.partition();

        if (topicId == null || partitionId < 0) {
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
        }

        SharePartitionKey mapKey = SharePartitionKey.getInstance(groupId, topicId, partitionId);
        if (leaderEpochMap.containsKey(mapKey, offset) && leaderEpochMap.get(mapKey, offset) > partitionData.leaderEpoch()) {
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.FENCED_LEADER_EPOCH, Errors.FENCED_LEADER_EPOCH.message()));
        }

        if (metadataImage != null && (metadataImage.topics().getTopic(topicId) == null ||
            metadataImage.topics().getPartition(topicId, partitionId) == null)) {
            return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
        }

        return Optional.empty();
    }

    private CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord> getWriteErrorResponse(
        Errors error,
        Uuid topicId,
        int partitionId
    ) {
        WriteShareGroupStateResponseData responseData = WriteShareGroupStateResponse.toErrorResponseData(topicId, partitionId, error, error.message());
        return new CoordinatorResult<>(Collections.emptyList(), responseData);
    }

    // Visible for testing
    Integer getLeaderMapValue(SharePartitionKey key) {
        return this.leaderEpochMap.get(key);
    }

    // Visible for testing
    Integer getStateEpochMapValue(SharePartitionKey key) {
        return this.stateEpochMap.get(key);
    }

    // Visible for testing
    ShareGroupOffset getShareStateMapValue(SharePartitionKey key) {
        return this.shareStateMap.get(key);
    }

    // Visible for testing
    CoordinatorMetricsShard getMetricsShard() {
        return metricsShard;
    }

    private static ShareGroupOffset merge(ShareGroupOffset soFar, ShareUpdateValue newData) {
        // snapshot epoch should be same as last share snapshot
        // state epoch is not present
        Set<PersisterOffsetsStateBatch> currentBatches = soFar.stateBatchAsSet();
        currentBatches.removeIf(batch -> batch.firstOffset() < newData.startOffset());

        return new ShareGroupOffset.Builder()
            .setSnapshotEpoch(soFar.snapshotEpoch())
            .setStateEpoch(soFar.stateEpoch())
            .setStartOffset(newData.startOffset() == -1 ? soFar.startOffset() : newData.startOffset())
            .setLeaderEpoch(newData.leaderEpoch() == -1 ? soFar.leaderEpoch() : newData.leaderEpoch())
            .setStateBatches(combineStateBatches(currentBatches, newData.stateBatches().stream()
                .map(PersisterOffsetsStateBatch::from)
                .collect(Collectors.toCollection(LinkedHashSet::new))))
            .build();
    }

    private static List<PersisterOffsetsStateBatch> combineStateBatches(
        Set<PersisterOffsetsStateBatch> currentBatch,
        Set<PersisterOffsetsStateBatch> newBatch
    ) {
        currentBatch.removeAll(newBatch);
        List<PersisterOffsetsStateBatch> batchesToAdd = new LinkedList<>(currentBatch);
        batchesToAdd.addAll(newBatch);
        return batchesToAdd;
    }

    private static ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
        if (apiMessageAndVersion == null) {
            return null;
        } else {
            return apiMessageAndVersion.message();
        }
    }
}
