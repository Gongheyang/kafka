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
package kafka.log.remote;

import com.yammer.metrics.core.Gauge;
import kafka.cluster.EndPoint;
import kafka.cluster.Partition;
import kafka.log.LogSegment;
import kafka.log.UnifiedLog;
import kafka.server.BrokerTopicStats;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.ClassLoaderAwareRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.internals.checkpoint.InMemoryLeaderEpochCheckpoint;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpoint;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.LazyIndex;
import org.apache.kafka.storage.internals.log.OffsetIndex;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.TimeIndex;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_CONSUMER_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_PRODUCER_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_STORAGE_THREAD_POOL_METRICS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RemoteLogManagerTest {
    Time time = new MockTime();
    int brokerId = 0;
    String logDir = TestUtils.tempDirectory("kafka-").toString();
    String clusterId = "dummyId";
    String remoteLogStorageTestProp = "remote.log.storage.test";
    String remoteLogStorageTestVal = "storage.test";
    String remoteLogMetadataTestProp = "remote.log.metadata.test";
    String remoteLogMetadataTestVal = "metadata.test";
    String remoteLogMetadataCommonClientTestProp = REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "common.client.test";
    String remoteLogMetadataCommonClientTestVal = "common.test";
    String remoteLogMetadataProducerTestProp = REMOTE_LOG_METADATA_PRODUCER_PREFIX + "producer.test";
    String remoteLogMetadataProducerTestVal = "producer.test";
    String remoteLogMetadataConsumerTestProp = REMOTE_LOG_METADATA_CONSUMER_PREFIX + "consumer.test";
    String remoteLogMetadataConsumerTestVal = "consumer.test";
    String remoteLogMetadataTopicPartitionsNum = "1";

    RemoteStorageManager remoteStorageManager = mock(RemoteStorageManager.class);
    RemoteLogMetadataManager remoteLogMetadataManager = mock(RemoteLogMetadataManager.class);
    RemoteLogManagerConfig remoteLogManagerConfig = null;

    BrokerTopicStats brokerTopicStats = null;
    RemoteLogManager remoteLogManager = null;

    TopicIdPartition leaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("Leader", 0));
    TopicIdPartition followerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("Follower", 0));
    Map<String, Uuid> topicIds = new HashMap<>();
    TopicPartition tp = new TopicPartition("TestTopic", 5);
    EpochEntry epochEntry0 = new EpochEntry(0, 0);
    EpochEntry epochEntry1 = new EpochEntry(1, 100);
    EpochEntry epochEntry2 = new EpochEntry(2, 200);
    List<EpochEntry> totalEpochEntries = Arrays.asList(epochEntry0, epochEntry1, epochEntry2);
    LeaderEpochCheckpoint checkpoint = new LeaderEpochCheckpoint() {
        List<EpochEntry> epochs = Collections.emptyList();

        @Override
        public void write(Collection<EpochEntry> epochs) {
            this.epochs = new ArrayList<>(epochs);
        }

        @Override
        public List<EpochEntry> read() {
            return epochs;
        }
    };

    UnifiedLog mockLog = mock(UnifiedLog.class);

    @BeforeEach
    void setUp() throws Exception {
        topicIds.put(leaderTopicIdPartition.topicPartition().topic(), leaderTopicIdPartition.topicId());
        topicIds.put(followerTopicIdPartition.topicPartition().topic(), followerTopicIdPartition.topicId());
        Properties props = kafka.utils.TestUtils.createDummyBrokerConfig();
        props.setProperty(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true");
        remoteLogManagerConfig = createRLMConfig(props);
        brokerTopicStats = new BrokerTopicStats(Optional.of(KafkaConfig.fromProps(props)));

        kafka.utils.TestUtils.clearYammerMetrics();
        remoteLogManager = new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, clusterId, time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats) {
            public RemoteStorageManager createRemoteStorageManager() {
                return remoteStorageManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        };
    }

    @Test
    void testGetLeaderEpochCheckpoint() {
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        InMemoryLeaderEpochCheckpoint inMemoryCheckpoint = remoteLogManager.getLeaderEpochCheckpoint(mockLog, 0, 300);
        assertEquals(totalEpochEntries, inMemoryCheckpoint.read());

        InMemoryLeaderEpochCheckpoint inMemoryCheckpoint2 = remoteLogManager.getLeaderEpochCheckpoint(mockLog, 100, 200);
        List<EpochEntry> epochEntries = inMemoryCheckpoint2.read();
        assertEquals(1, epochEntries.size());
        assertEquals(epochEntry1, epochEntries.get(0));
    }

    @Test
    void testFindHighestRemoteOffset() throws RemoteStorageException {
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(tp, checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        TopicIdPartition tpId = new TopicIdPartition(Uuid.randomUuid(), tp);
        long offset = remoteLogManager.findHighestRemoteOffset(tpId, mockLog);
        assertEquals(-1, offset);

        when(remoteLogMetadataManager.highestOffsetForEpoch(tpId, 2)).thenReturn(Optional.of(200L));
        long offset2 = remoteLogManager.findHighestRemoteOffset(tpId, mockLog);
        assertEquals(200, offset2);
    }

    @Test
    void testRemoteLogMetadataManagerWithUserDefinedConfigs() {
        String key = "key";
        String configPrefix = "config.prefix";
        Properties props = new Properties();
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, configPrefix);
        props.put(configPrefix + key, "world");
        props.put("remote.log.metadata.y", "z");

        Map<String, Object> metadataMangerConfig = createRLMConfig(props).remoteLogMetadataManagerProps();
        assertEquals(props.get(configPrefix + key), metadataMangerConfig.get(key));
        assertFalse(metadataMangerConfig.containsKey("remote.log.metadata.y"));
    }

    @Test
    void testRemoteStorageManagerWithUserDefinedConfigs() {
        String key = "key";
        String configPrefix = "config.prefix";
        Properties props = new Properties();
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP, configPrefix);
        props.put(configPrefix + key, "world");
        props.put("remote.storage.manager.y", "z");

        Map<String, Object> remoteStorageManagerConfig = createRLMConfig(props).remoteStorageManagerProps();
        assertEquals(props.get(configPrefix + key), remoteStorageManagerConfig.get(key));
        assertFalse(remoteStorageManagerConfig.containsKey("remote.storage.manager.y"));
    }

    @Test
    void testRemoteLogMetadataManagerWithEndpointConfig() {
        String host = "localhost";
        String port = "1234";
        String securityProtocol = "PLAINTEXT";
        EndPoint endPoint = new EndPoint(host, Integer.parseInt(port), new ListenerName(securityProtocol),
                SecurityProtocol.PLAINTEXT);
        remoteLogManager.onEndPointCreated(endPoint);
        remoteLogManager.startup();

        ArgumentCaptor<Map<String, Object>> capture = ArgumentCaptor.forClass(Map.class);
        verify(remoteLogMetadataManager, times(1)).configure(capture.capture());
        assertEquals(host + ":" + port, capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "bootstrap.servers"));
        assertEquals(securityProtocol, capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol"));
        assertEquals(clusterId, capture.getValue().get("cluster.id"));
        assertEquals(brokerId, capture.getValue().get(KafkaConfig.BrokerIdProp()));
    }

    @Test
    void testRemoteLogMetadataManagerWithEndpointConfigOverridden() throws IOException {
        Properties props = new Properties();
        // override common security.protocol by adding "RLMM prefix" and "remote log metadata common client prefix"
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol", "SSL");
        try (RemoteLogManager remoteLogManager = new RemoteLogManager(
                createRLMConfig(props),
                brokerId,
                logDir,
                clusterId,
                time,
                tp -> Optional.of(mockLog),
                (topicPartition, offset) -> { },
                brokerTopicStats) {
            public RemoteStorageManager createRemoteStorageManager() {
                return remoteStorageManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        }) {

            String host = "localhost";
            String port = "1234";
            String securityProtocol = "PLAINTEXT";
            EndPoint endPoint = new EndPoint(host, Integer.parseInt(port), new ListenerName(securityProtocol),
                    SecurityProtocol.PLAINTEXT);
            remoteLogManager.onEndPointCreated(endPoint);
            remoteLogManager.startup();

            ArgumentCaptor<Map<String, Object>> capture = ArgumentCaptor.forClass(Map.class);
            verify(remoteLogMetadataManager, times(1)).configure(capture.capture());
            assertEquals(host + ":" + port, capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "bootstrap.servers"));
            // should be overridden as SSL
            assertEquals("SSL", capture.getValue().get(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol"));
            assertEquals(clusterId, capture.getValue().get("cluster.id"));
            assertEquals(brokerId, capture.getValue().get(KafkaConfig.BrokerIdProp()));
        }
    }



    @Test
    void testStartup() {
        remoteLogManager.startup();
        ArgumentCaptor<Map<String, Object>> capture = ArgumentCaptor.forClass(Map.class);
        verify(remoteStorageManager, times(1)).configure(capture.capture());
        assertEquals(brokerId, capture.getValue().get("broker.id"));
        assertEquals(remoteLogStorageTestVal, capture.getValue().get(remoteLogStorageTestProp));

        verify(remoteLogMetadataManager, times(1)).configure(capture.capture());
        assertEquals(brokerId, capture.getValue().get("broker.id"));
        assertEquals(logDir, capture.getValue().get("log.dir"));

        // verify the configs starting with "remote.log.metadata", "remote.log.metadata.common.client."
        // "remote.log.metadata.producer.", and "remote.log.metadata.consumer." are correctly passed in
        assertEquals(remoteLogMetadataTopicPartitionsNum, capture.getValue().get(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP));
        assertEquals(remoteLogMetadataTestVal, capture.getValue().get(remoteLogMetadataTestProp));
        assertEquals(remoteLogMetadataConsumerTestVal, capture.getValue().get(remoteLogMetadataConsumerTestProp));
        assertEquals(remoteLogMetadataProducerTestVal, capture.getValue().get(remoteLogMetadataProducerTestProp));
        assertEquals(remoteLogMetadataCommonClientTestVal, capture.getValue().get(remoteLogMetadataCommonClientTestProp));
    }

    // This test creates 2 log segments, 1st one has start offset of 0, 2nd one (and active one) has start offset of 150.
    // The leader epochs are [0->0, 1->100, 2->200]. We are verifying:
    // 1. There's only 1 segment copied to remote storage
    // 2. The segment got copied to remote storage is the old segment, not the active one
    // 3. The log segment metadata stored into remoteLogMetadataManager is what we expected, both before and after copying the log segments
    // 4. The log segment got copied to remote storage has the expected metadata
    // 5. The highest remote offset is updated to the expected value
    @Test
    void testCopyLogSegmentsToRemoteShouldCopyExpectedLogSegment() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        long lso = 250L;
        long leo = 300L;
        assertCopyExpectedLogSegmentsToRemote(oldSegmentStartOffset, nextSegmentStartOffset, lso, leo);
    }

    /**
     * The following values will be equal when the active segment gets rotated to passive and there are no new messages:
     * last-stable-offset = high-water-mark = log-end-offset = base-offset-of-active-segment.
     * This test asserts that the active log segment that was rotated after log.roll.ms are copied to remote storage.
     */
    @Test
    void testCopyLogSegmentToRemoteForStaleTopic() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        long lso = 150L;
        long leo = 150L;
        assertCopyExpectedLogSegmentsToRemote(oldSegmentStartOffset, nextSegmentStartOffset, lso, leo);
    }

    private void assertCopyExpectedLogSegmentsToRemote(long oldSegmentStartOffset,
                                                       long nextSegmentStartOffset,
                                                       long lastStableOffset,
                                                       long logEndOffset) throws Exception {
        long oldSegmentEndOffset = nextSegmentStartOffset - 1;
        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(-1L));

        File tempFile = TestUtils.tempFile();
        File mockProducerSnapshotIndex = TestUtils.tempFile();
        File tempDir = TestUtils.tempDirectory();
        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);
        verify(oldSegment, times(0)).readNextOffset();
        verify(activeSegment, times(0)).readNextOffset();

        FileRecords fileRecords = mock(FileRecords.class);
        when(oldSegment.log()).thenReturn(fileRecords);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldSegment, activeSegment)));

        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(lastStableOffset);
        when(mockLog.logEndOffset()).thenReturn(logEndOffset);

        LazyIndex idx = LazyIndex.forOffset(UnifiedLog.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000);
        LazyIndex timeIdx = LazyIndex.forTime(UnifiedLog.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500);
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.lazyTimeIndex()).thenReturn(timeIdx);
        when(oldSegment.lazyOffsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class))).thenReturn(dummyFuture);
        when(remoteStorageManager.copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class)))
                .thenReturn(Optional.empty());

        // Verify the metrics for remote writes and for failures is zero before attempt to copy log segment
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyBytesRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyBytesRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        task.convertToLeader(2);
        task.copyLogSegmentsToRemote(mockLog);

        // verify remoteLogMetadataManager did add the expected RemoteLogSegmentMetadata
        ArgumentCaptor<RemoteLogSegmentMetadata> remoteLogSegmentMetadataArg = ArgumentCaptor.forClass(RemoteLogSegmentMetadata.class);
        verify(remoteLogMetadataManager).addRemoteLogSegmentMetadata(remoteLogSegmentMetadataArg.capture());
        // The old segment should only contain leader epoch [0->0, 1->100] since its offset range is [0, 149]
        Map<Integer, Long> expectedLeaderEpochs = new TreeMap<>();
        expectedLeaderEpochs.put(epochEntry0.epoch, epochEntry0.startOffset);
        expectedLeaderEpochs.put(epochEntry1.epoch, epochEntry1.startOffset);
        verifyRemoteLogSegmentMetadata(remoteLogSegmentMetadataArg.getValue(), oldSegmentStartOffset, oldSegmentEndOffset, expectedLeaderEpochs);

        // verify copyLogSegmentData is passing the RemoteLogSegmentMetadata we created above
        // and verify the logSegmentData passed is expected
        ArgumentCaptor<RemoteLogSegmentMetadata> remoteLogSegmentMetadataArg2 = ArgumentCaptor.forClass(RemoteLogSegmentMetadata.class);
        ArgumentCaptor<LogSegmentData> logSegmentDataArg = ArgumentCaptor.forClass(LogSegmentData.class);
        verify(remoteStorageManager, times(1)).copyLogSegmentData(remoteLogSegmentMetadataArg2.capture(), logSegmentDataArg.capture());
        assertEquals(remoteLogSegmentMetadataArg.getValue(), remoteLogSegmentMetadataArg2.getValue());
        // The old segment should only contain leader epoch [0->0, 1->100] since its offset range is [0, 149]
        verifyLogSegmentData(logSegmentDataArg.getValue(), idx, timeIdx, txnIndex, tempFile, mockProducerSnapshotIndex,
            Arrays.asList(epochEntry0, epochEntry1));

        // verify remoteLogMetadataManager did add the expected RemoteLogSegmentMetadataUpdate
        ArgumentCaptor<RemoteLogSegmentMetadataUpdate> remoteLogSegmentMetadataUpdateArg = ArgumentCaptor.forClass(RemoteLogSegmentMetadataUpdate.class);
        verify(remoteLogMetadataManager, times(1)).updateRemoteLogSegmentMetadata(remoteLogSegmentMetadataUpdateArg.capture());
        verifyRemoteLogSegmentMetadataUpdate(remoteLogSegmentMetadataUpdateArg.getValue());

        // verify the highest remote offset is updated to the expected value
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        verify(mockLog, times(1)).updateHighestOffsetInRemoteStorage(argument.capture());
        assertEquals(oldSegmentEndOffset, argument.getValue());

        // Verify the metric for remote writes is updated correctly
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(10, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyBytesRate().count());
        // Verify we did not report any failure for remote writes
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(1, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(10, brokerTopicStats.allTopicsStats().remoteCopyBytesRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
    }

    // We are verifying that if the size of a piece of custom metadata is bigger than the configured limit,
    // the copy task should be cancelled and there should be an attempt to delete the just copied segment.
    @Test
    void testCustomMetadataSizeExceedsLimit() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        long lastStableOffset = 150L;
        long logEndOffset = 150L;

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(-1L));

        File tempFile = TestUtils.tempFile();
        File mockProducerSnapshotIndex = TestUtils.tempFile();
        File tempDir = TestUtils.tempDirectory();
        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);
        verify(oldSegment, times(0)).readNextOffset();
        verify(activeSegment, times(0)).readNextOffset();

        FileRecords fileRecords = mock(FileRecords.class);
        when(oldSegment.log()).thenReturn(fileRecords);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldSegment, activeSegment)));

        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(lastStableOffset);
        when(mockLog.logEndOffset()).thenReturn(logEndOffset);

        LazyIndex idx = LazyIndex.forOffset(UnifiedLog.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000);
        LazyIndex timeIdx = LazyIndex.forTime(UnifiedLog.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500);
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.lazyTimeIndex()).thenReturn(timeIdx);
        when(oldSegment.lazyOffsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        int customMetadataSizeLimit = 128;
        CustomMetadata customMetadata = new CustomMetadata(new byte[customMetadataSizeLimit * 2]);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        when(remoteStorageManager.copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class)))
                .thenReturn(Optional.of(customMetadata));

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, customMetadataSizeLimit);
        task.convertToLeader(2);
        task.copyLogSegmentsToRemote(mockLog);

        ArgumentCaptor<RemoteLogSegmentMetadata> remoteLogSegmentMetadataArg = ArgumentCaptor.forClass(RemoteLogSegmentMetadata.class);
        verify(remoteLogMetadataManager).addRemoteLogSegmentMetadata(remoteLogSegmentMetadataArg.capture());

        // Check we attempt to delete the segment data providing the custom metadata back.
        RemoteLogSegmentMetadataUpdate expectedMetadataUpdate = new RemoteLogSegmentMetadataUpdate(
                remoteLogSegmentMetadataArg.getValue().remoteLogSegmentId(), time.milliseconds(),
                Optional.of(customMetadata), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);
        RemoteLogSegmentMetadata expectedDeleteMetadata = remoteLogSegmentMetadataArg.getValue().createWithUpdates(expectedMetadataUpdate);
        verify(remoteStorageManager, times(1)).deleteLogSegmentData(eq(expectedDeleteMetadata));

        // Check the task is cancelled in the end.
        assertTrue(task.isCancelled());

        // The metadata update should not be posted.
        verify(remoteLogMetadataManager, never()).updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class));

        // Verify the metric for remote writes are not updated.
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyBytesRate().count());
        // Verify we did not report any failure for remote writes
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(1, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyBytesRate().count());
        assertEquals(1, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
    }

    @Test
    void testRemoteLogManagerTasksAvgIdlePercentMetrics() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;
        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(0L));

        File tempFile = TestUtils.tempFile();
        File mockProducerSnapshotIndex = TestUtils.tempFile();
        File tempDir = TestUtils.tempDirectory();
        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);

        FileRecords fileRecords = mock(FileRecords.class);
        when(oldSegment.log()).thenReturn(fileRecords);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldSegment, activeSegment)));

        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(250L);

        LazyIndex idx = LazyIndex.forOffset(UnifiedLog.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000);
        LazyIndex timeIdx = LazyIndex.forTime(UnifiedLog.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500);
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.lazyTimeIndex()).thenReturn(timeIdx);
        when(oldSegment.lazyOffsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class))).thenReturn(dummyFuture);

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(ans -> {
            // waiting for verification
            latch.await();
            return null;
        }).when(remoteStorageManager).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));
        Partition mockLeaderPartition = mockPartition(leaderTopicIdPartition);
        Partition mockFollowerPartition = mockPartition(followerTopicIdPartition);

        // before running tasks, the remote log manager tasks should be all idle
        assertEquals(1.0, yammerMetricValue("RemoteLogManagerTasksAvgIdlePercent"));
        remoteLogManager.onLeadershipChange(Collections.singleton(mockLeaderPartition), Collections.singleton(mockFollowerPartition), topicIds);
        assertTrue(yammerMetricValue("RemoteLogManagerTasksAvgIdlePercent") < 1.0);
        // unlock copyLogSegmentData
        latch.countDown();
    }

    private double yammerMetricValue(String name) {
        Gauge<Double> guage = (Gauge) KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                .filter(e -> e.getKey().getMBeanName().contains(name))
                .findFirst()
                .get()
                .getValue();
        return guage.value();
    }

    @Test
    void testMetricsUpdateOnCopyLogSegmentsFailure() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;

        when(mockLog.topicPartition()).thenReturn(leaderTopicIdPartition.topicPartition());

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(0L));

        File tempFile = TestUtils.tempFile();
        File mockProducerSnapshotIndex = TestUtils.tempFile();
        File tempDir = TestUtils.tempDirectory();
        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);

        FileRecords fileRecords = mock(FileRecords.class);
        when(oldSegment.log()).thenReturn(fileRecords);
        when(fileRecords.file()).thenReturn(tempFile);
        when(fileRecords.sizeInBytes()).thenReturn(10);
        when(oldSegment.readNextOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldSegment, activeSegment)));

        ProducerStateManager mockStateManager = mock(ProducerStateManager.class);
        when(mockLog.producerStateManager()).thenReturn(mockStateManager);
        when(mockStateManager.fetchSnapshot(anyLong())).thenReturn(Optional.of(mockProducerSnapshotIndex));
        when(mockLog.lastStableOffset()).thenReturn(250L);

        LazyIndex idx = LazyIndex.forOffset(UnifiedLog.offsetIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1000);
        LazyIndex timeIdx = LazyIndex.forTime(UnifiedLog.timeIndexFile(tempDir, oldSegmentStartOffset, ""), oldSegmentStartOffset, 1500);
        File txnFile = UnifiedLog.transactionIndexFile(tempDir, oldSegmentStartOffset, "");
        txnFile.createNewFile();
        TransactionIndex txnIndex = new TransactionIndex(oldSegmentStartOffset, txnFile);
        when(oldSegment.lazyTimeIndex()).thenReturn(timeIdx);
        when(oldSegment.lazyOffsetIndex()).thenReturn(idx);
        when(oldSegment.txnIndex()).thenReturn(txnIndex);

        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class))).thenReturn(dummyFuture);
        doThrow(new RuntimeException()).when(remoteStorageManager).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));

        // Verify the metrics for remote write requests/failures is zero before attempt to copy log segment
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(0, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(0, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        task.convertToLeader(2);
        task.copyLogSegmentsToRemote(mockLog);

        // Verify we attempted to copy log segment metadata to remote storage
        verify(remoteStorageManager, times(1)).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));

        // Verify we should not have updated the highest offset because of write failure
        verify(mockLog, times(0)).updateHighestOffsetInRemoteStorage(anyLong());
        // Verify the metric for remote write requests/failures was updated.
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).remoteCopyRequestRate().count());
        assertEquals(1, brokerTopicStats.topicStats(leaderTopicIdPartition.topic()).failedRemoteCopyRequestRate().count());
        // Verify aggregate metrics
        assertEquals(1, brokerTopicStats.allTopicsStats().remoteCopyRequestRate().count());
        assertEquals(1, brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().count());
    }

    @Test
    void testCopyLogSegmentsToRemoteShouldNotCopySegmentForFollower() throws Exception {
        long oldSegmentStartOffset = 0L;
        long nextSegmentStartOffset = 150L;

        // leader epoch preparation
        checkpoint.write(totalEpochEntries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(leaderTopicIdPartition.topicPartition(), checkpoint);
        when(mockLog.leaderEpochCache()).thenReturn(Option.apply(cache));
        when(remoteLogMetadataManager.highestOffsetForEpoch(any(TopicIdPartition.class), anyInt())).thenReturn(Optional.of(0L));

        // create 2 log segments, with 0 and 150 as log start offset
        LogSegment oldSegment = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(oldSegment.baseOffset()).thenReturn(oldSegmentStartOffset);
        when(activeSegment.baseOffset()).thenReturn(nextSegmentStartOffset);

        when(mockLog.activeSegment()).thenReturn(activeSegment);
        when(mockLog.logStartOffset()).thenReturn(oldSegmentStartOffset);
        when(mockLog.logSegments(anyLong(), anyLong())).thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(oldSegment, activeSegment)));
        when(mockLog.lastStableOffset()).thenReturn(250L);

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        task.convertToFollower();
        task.copyLogSegmentsToRemote(mockLog);

        // verify the remoteLogMetadataManager never add any metadata and remoteStorageManager never copy log segments
        verify(remoteLogMetadataManager, never()).addRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadata.class));
        verify(remoteStorageManager, never()).copyLogSegmentData(any(RemoteLogSegmentMetadata.class), any(LogSegmentData.class));
        verify(remoteLogMetadataManager, never()).updateRemoteLogSegmentMetadata(any(RemoteLogSegmentMetadataUpdate.class));
        verify(mockLog, never()).updateHighestOffsetInRemoteStorage(anyLong());
    }

    private void verifyRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                long oldSegmentStartOffset,
                                                long oldSegmentEndOffset,
                                                Map<Integer, Long> expectedLeaderEpochs) {
        assertEquals(leaderTopicIdPartition, remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition());
        assertEquals(oldSegmentStartOffset, remoteLogSegmentMetadata.startOffset());
        assertEquals(oldSegmentEndOffset, remoteLogSegmentMetadata.endOffset());

        NavigableMap<Integer, Long> leaderEpochs = remoteLogSegmentMetadata.segmentLeaderEpochs();
        assertEquals(expectedLeaderEpochs.size(), leaderEpochs.size());
        Iterator<Map.Entry<Integer, Long>> leaderEpochEntries = expectedLeaderEpochs.entrySet().iterator();
        assertEquals(leaderEpochEntries.next(), leaderEpochs.firstEntry());
        assertEquals(leaderEpochEntries.next(), leaderEpochs.lastEntry());

        assertEquals(brokerId, remoteLogSegmentMetadata.brokerId());
        assertEquals(RemoteLogSegmentState.COPY_SEGMENT_STARTED, remoteLogSegmentMetadata.state());
    }

    private void verifyRemoteLogSegmentMetadataUpdate(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate) {
        assertEquals(leaderTopicIdPartition, remoteLogSegmentMetadataUpdate.remoteLogSegmentId().topicIdPartition());
        assertEquals(brokerId, remoteLogSegmentMetadataUpdate.brokerId());

        assertEquals(RemoteLogSegmentState.COPY_SEGMENT_FINISHED, remoteLogSegmentMetadataUpdate.state());
    }

    private void verifyLogSegmentData(LogSegmentData logSegmentData,
                                      LazyIndex idx,
                                      LazyIndex timeIdx,
                                      TransactionIndex txnIndex,
                                      File tempFile,
                                      File mockProducerSnapshotIndex,
                                      List<EpochEntry> expectedLeaderEpoch) throws IOException {
        assertEquals(idx.file().getAbsolutePath(), logSegmentData.offsetIndex().toAbsolutePath().toString());
        assertEquals(timeIdx.file().getAbsolutePath(), logSegmentData.timeIndex().toAbsolutePath().toString());
        assertEquals(txnIndex.file().getPath(), logSegmentData.transactionIndex().get().toAbsolutePath().toString());
        assertEquals(tempFile.getAbsolutePath(), logSegmentData.logSegment().toAbsolutePath().toString());
        assertEquals(mockProducerSnapshotIndex.getAbsolutePath(), logSegmentData.producerSnapshotIndex().toAbsolutePath().toString());

        InMemoryLeaderEpochCheckpoint inMemoryLeaderEpochCheckpoint = new InMemoryLeaderEpochCheckpoint();
        inMemoryLeaderEpochCheckpoint.write(expectedLeaderEpoch);
        assertEquals(inMemoryLeaderEpochCheckpoint.readAsByteBuffer(), logSegmentData.leaderEpochIndex());
    }

    @Test
    void testGetClassLoaderAwareRemoteStorageManager() throws Exception {
        ClassLoaderAwareRemoteStorageManager rsmManager = mock(ClassLoaderAwareRemoteStorageManager.class);
        try (RemoteLogManager remoteLogManager =
            new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, clusterId, time,
                    t -> Optional.empty(),
                    (topicPartition, offset) -> { },
                    brokerTopicStats) {
                public RemoteStorageManager createRemoteStorageManager() {
                    return rsmManager;
                }
            };
        ) {
            assertEquals(rsmManager, remoteLogManager.storageManager());
        }
    }

    private void verifyInCache(TopicIdPartition... topicIdPartitions) {
        Arrays.stream(topicIdPartitions).forEach(topicIdPartition -> {
            assertDoesNotThrow(() -> remoteLogManager.fetchRemoteLogSegmentMetadata(topicIdPartition.topicPartition(), 0, 0L));
        });
    }

    private void verifyNotInCache(TopicIdPartition... topicIdPartitions) {
        Arrays.stream(topicIdPartitions).forEach(topicIdPartition -> {
            assertThrows(KafkaException.class, () ->
                remoteLogManager.fetchRemoteLogSegmentMetadata(topicIdPartition.topicPartition(), 0, 0L));
        });
    }

    @Test
    void testTopicIdCacheUpdates() throws RemoteStorageException {
        Partition mockLeaderPartition = mockPartition(leaderTopicIdPartition);
        Partition mockFollowerPartition = mockPartition(followerTopicIdPartition);

        when(remoteLogMetadataManager.remoteLogSegmentMetadata(any(TopicIdPartition.class), anyInt(), anyLong()))
            .thenReturn(Optional.empty());
        verifyNotInCache(followerTopicIdPartition, leaderTopicIdPartition);
        // Load topicId cache
        remoteLogManager.onLeadershipChange(Collections.singleton(mockLeaderPartition), Collections.singleton(mockFollowerPartition), topicIds);
        verify(remoteLogMetadataManager, times(1))
            .onPartitionLeadershipChanges(Collections.singleton(leaderTopicIdPartition), Collections.singleton(followerTopicIdPartition));
        verifyInCache(followerTopicIdPartition, leaderTopicIdPartition);

        // Evicts from topicId cache
        remoteLogManager.stopPartitions(Collections.singleton(leaderTopicIdPartition.topicPartition()), true, (tp, ex) -> { });
        verifyNotInCache(leaderTopicIdPartition);
        verifyInCache(followerTopicIdPartition);

        // Evicts from topicId cache
        remoteLogManager.stopPartitions(Collections.singleton(followerTopicIdPartition.topicPartition()), true, (tp, ex) -> { });
        verifyNotInCache(leaderTopicIdPartition, followerTopicIdPartition);
    }

    @Test
    void testFetchRemoteLogSegmentMetadata() throws RemoteStorageException {
        remoteLogManager.onLeadershipChange(
            Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        remoteLogManager.fetchRemoteLogSegmentMetadata(leaderTopicIdPartition.topicPartition(), 10, 100L);
        remoteLogManager.fetchRemoteLogSegmentMetadata(followerTopicIdPartition.topicPartition(), 20, 200L);

        verify(remoteLogMetadataManager)
            .remoteLogSegmentMetadata(eq(leaderTopicIdPartition), anyInt(), anyLong());
        verify(remoteLogMetadataManager)
            .remoteLogSegmentMetadata(eq(followerTopicIdPartition), anyInt(), anyLong());
    }

    @Test
    void testOnLeadershipChangeWillInvokeHandleLeaderOrFollowerPartitions() {
        RemoteLogManager spyRemoteLogManager = spy(remoteLogManager);
        spyRemoteLogManager.onLeadershipChange(
            Collections.emptySet(), Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        verify(spyRemoteLogManager).doHandleLeaderOrFollowerPartitions(eq(followerTopicIdPartition), any(java.util.function.Consumer.class));

        Mockito.reset(spyRemoteLogManager);

        spyRemoteLogManager.onLeadershipChange(
            Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.emptySet(), topicIds);
        verify(spyRemoteLogManager).doHandleLeaderOrFollowerPartitions(eq(leaderTopicIdPartition), any(java.util.function.Consumer.class));
    }

    private MemoryRecords records(long timestamp,
                                  long initialOffset,
                                  int partitionLeaderEpoch) {
        return MemoryRecords.withRecords(initialOffset, CompressionType.NONE, partitionLeaderEpoch,
            new SimpleRecord(timestamp - 1, "first message".getBytes()),
            new SimpleRecord(timestamp + 1, "second message".getBytes()),
            new SimpleRecord(timestamp + 2, "third message".getBytes())
            );
    }

    @Test
    void testRLMTaskShouldSetLeaderEpochCorrectly() {
        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        assertFalse(task.isLeader());
        task.convertToLeader(1);
        assertTrue(task.isLeader());
        task.convertToFollower();
        assertFalse(task.isLeader());
    }

    @Test
    void testFindOffsetByTimestamp() throws IOException, RemoteStorageException {
        TopicPartition tp = leaderTopicIdPartition.topicPartition();
        RemoteLogSegmentId remoteLogSegmentId = new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid());
        long ts = time.milliseconds();
        long startOffset = 120;
        int targetLeaderEpoch = 10;

        RemoteLogSegmentMetadata segmentMetadata = mock(RemoteLogSegmentMetadata.class);
        when(segmentMetadata.remoteLogSegmentId()).thenReturn(remoteLogSegmentId);
        when(segmentMetadata.maxTimestampMs()).thenReturn(ts + 2);
        when(segmentMetadata.startOffset()).thenReturn(startOffset);
        when(segmentMetadata.endOffset()).thenReturn(startOffset + 2);

        File tpDir = new File(logDir, tp.toString());
        Files.createDirectory(tpDir.toPath());
        File txnIdxFile = new File(tpDir, "txn-index" + UnifiedLog.TxnIndexFileSuffix());
        txnIdxFile.createNewFile();
        when(remoteStorageManager.fetchIndex(any(RemoteLogSegmentMetadata.class), any(IndexType.class)))
            .thenAnswer(ans -> {
                RemoteLogSegmentMetadata metadata = ans.<RemoteLogSegmentMetadata>getArgument(0);
                IndexType indexType = ans.<IndexType>getArgument(1);
                int maxEntries = (int) (metadata.endOffset() - metadata.startOffset());
                OffsetIndex offsetIdx = new OffsetIndex(new File(tpDir, String.valueOf(metadata.startOffset()) + UnifiedLog.IndexFileSuffix()),
                    metadata.startOffset(), maxEntries * 8);
                TimeIndex timeIdx = new TimeIndex(new File(tpDir, String.valueOf(metadata.startOffset()) + UnifiedLog.TimeIndexFileSuffix()),
                    metadata.startOffset(), maxEntries * 12);
                switch (indexType) {
                    case OFFSET:
                        return new FileInputStream(offsetIdx.file());
                    case TIMESTAMP:
                        return new FileInputStream(timeIdx.file());
                    case TRANSACTION:
                        return new FileInputStream(txnIdxFile);
                }
                return null;
            });

        when(remoteLogMetadataManager.listRemoteLogSegments(eq(leaderTopicIdPartition), anyInt()))
            .thenAnswer(ans -> {
                int leaderEpoch = ans.<Integer>getArgument(1);
                if (leaderEpoch == targetLeaderEpoch)
                    return Collections.singleton(segmentMetadata).iterator();
                else
                    return Collections.emptyList().iterator();
            });

        // 3 messages are added with offset, and timestamp as below
        // startOffset   , ts-1
        // startOffset+1 , ts+1
        // startOffset+2 , ts+2
        when(remoteStorageManager.fetchLogSegment(segmentMetadata, 0))
            .thenAnswer(a -> new ByteArrayInputStream(records(ts, startOffset, targetLeaderEpoch).buffer().array()));

        LeaderEpochFileCache leaderEpochFileCache = new LeaderEpochFileCache(tp, checkpoint);
        leaderEpochFileCache.assign(5, 99L);
        leaderEpochFileCache.assign(targetLeaderEpoch, startOffset);
        leaderEpochFileCache.assign(12, 500L);

        remoteLogManager.onLeadershipChange(Collections.singleton(mockPartition(leaderTopicIdPartition)), Collections.emptySet(), topicIds);
        // Fetching message for timestamp `ts` will return the message with startOffset+1, and `ts+1` as there are no
        // messages starting with the startOffset and with `ts`.
        Optional<FileRecords.TimestampAndOffset> maybeTimestampAndOffset1 = remoteLogManager.findOffsetByTimestamp(tp, ts, startOffset, leaderEpochFileCache);
        assertEquals(Optional.of(new FileRecords.TimestampAndOffset(ts + 1, startOffset + 1, Optional.of(targetLeaderEpoch))), maybeTimestampAndOffset1);

        // Fetching message for `ts+2` will return the message with startOffset+2 and its timestamp value is `ts+2`.
        Optional<FileRecords.TimestampAndOffset> maybeTimestampAndOffset2 = remoteLogManager.findOffsetByTimestamp(tp, ts + 2, startOffset, leaderEpochFileCache);
        assertEquals(Optional.of(new FileRecords.TimestampAndOffset(ts + 2, startOffset + 2, Optional.of(targetLeaderEpoch))), maybeTimestampAndOffset2);

        // Fetching message for `ts+3` will return None as there are no records with timestamp >= ts+3.
        Optional<FileRecords.TimestampAndOffset>  maybeTimestampAndOffset3 = remoteLogManager.findOffsetByTimestamp(tp, ts + 3, startOffset, leaderEpochFileCache);
        assertEquals(Optional.empty(), maybeTimestampAndOffset3);
    }

    @Test
    void testIdempotentClose() throws IOException {
        remoteLogManager.close();
        remoteLogManager.close();
        InOrder inorder = inOrder(remoteStorageManager, remoteLogMetadataManager);
        inorder.verify(remoteStorageManager, times(1)).close();
        inorder.verify(remoteLogMetadataManager, times(1)).close();
    }

    @Test
    public void testRemoveMetricsOnClose() throws IOException {
        MockedConstruction<KafkaMetricsGroup> mockMetricsGroupCtor = mockConstruction(KafkaMetricsGroup.class);
        try {
            RemoteLogManager remoteLogManager = new RemoteLogManager(remoteLogManagerConfig, brokerId, logDir, clusterId,
                time, tp -> Optional.of(mockLog), (topicPartition, offset) -> { }, brokerTopicStats) {
                public RemoteStorageManager createRemoteStorageManager() {
                    return remoteStorageManager;
                }

                public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                    return remoteLogMetadataManager;
                }
            };
            // Close RemoteLogManager so that metrics are removed
            remoteLogManager.close();

            KafkaMetricsGroup mockRlmMetricsGroup = mockMetricsGroupCtor.constructed().get(0);
            KafkaMetricsGroup mockThreadPoolMetricsGroup = mockMetricsGroupCtor.constructed().get(1);

            List<String> remoteLogManagerMetricNames = Collections.singletonList(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC.getName());
            Set<String> remoteStorageThreadPoolMetricNames = REMOTE_STORAGE_THREAD_POOL_METRICS;

            verify(mockRlmMetricsGroup, times(remoteLogManagerMetricNames.size())).newGauge(anyString(), any());
            // Verify that the RemoteLogManager metrics are removed
            remoteLogManagerMetricNames.forEach(metricName -> verify(mockRlmMetricsGroup).removeMetric(metricName));

            verify(mockThreadPoolMetricsGroup, times(remoteStorageThreadPoolMetricNames.size())).newGauge(anyString(), any());
            // Verify that the RemoteStorageThreadPool metrics are removed
            remoteStorageThreadPoolMetricNames.forEach(metricName -> verify(mockThreadPoolMetricsGroup).removeMetric(metricName));

            verifyNoMoreInteractions(mockRlmMetricsGroup);
            verifyNoMoreInteractions(mockThreadPoolMetricsGroup);
        } finally {
            mockMetricsGroupCtor.close();
        }
    }

    private static RemoteLogSegmentMetadata createRemoteLogSegmentMetadata(long startOffset, long endOffset, Map<Integer, Long> segmentEpochs) {
        return new RemoteLogSegmentMetadata(
                new RemoteLogSegmentId(new TopicIdPartition(Uuid.randomUuid(),
                        new TopicPartition("topic", 0)), Uuid.randomUuid()),
                startOffset, endOffset,
                100000L,
                1,
                100000L,
                1000,
                Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, segmentEpochs);
    }

    @Test
    public void testRemoteSegmentWithinLeaderEpochs() {
        // Test whether a remote segment is within the leader epochs
        final long logEndOffset = 90L;

        TreeMap<Integer, Long> leaderEpochToStartOffset = new TreeMap<Integer, Long>() {{
                put(0, 0L);
                put(1, 10L);
                put(2, 20L);
                put(3, 30L);
                put(4, 40L);
                put(5, 50L);
                put(7, 70L);
            }};

        // Test whether a remote segment's epochs/offsets(multiple) are within the range of leader epochs
        assertTrue(RemoteLogManager.isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                35,
                new TreeMap<Integer, Long>() {{
                    put(1, 15L);
                    put(2, 20L);
                    put(3, 30L);
                }}), logEndOffset, leaderEpochToStartOffset));

        // Test whether a remote segment's epochs/offsets(single) are within the range of leader epochs
        assertTrue(RemoteLogManager.isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                19,
                new TreeMap<Integer, Long>() {{
                    put(1, 15L);
                }}), logEndOffset, leaderEpochToStartOffset));

        // Test whether a remote segment's epochs/offsets(single) are within the range of leader epochs
        assertTrue(RemoteLogManager.isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                19,
                new TreeMap<Integer, Long>() {{
                    put(1, 15L);
                }}), logEndOffset, leaderEpochToStartOffset));

        // Test whether a remote segment's start offset is same as the offset of the respective leader epoch entry.
        assertTrue(RemoteLogManager.isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                0,
                5,
                new TreeMap<Integer, Long>() {{
                    put(0, 0L); // same as leader epoch's start offset
                }}), logEndOffset, leaderEpochToStartOffset));

        // Test whether a remote segment's start offset is same as the offset of the respective leader epoch entry.
        assertTrue(RemoteLogManager.isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                70,
                75,
                new TreeMap<Integer, Long>() {{
                    put(7, 70L); // same as leader epoch's start offset
                }}), logEndOffset, leaderEpochToStartOffset));


        // Test whether a remote segment's end offset is same as the end offset of the respective leader epoch entry.
        assertTrue(RemoteLogManager.isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                29, // same as end offset for epoch 2 in leaderEpochToStartOffset
                new TreeMap<Integer, Long>() {{
                    put(1, 15L);
                    put(2, 20L);
                }}), logEndOffset, leaderEpochToStartOffset));

        // Test whether any of the epoch's is not with in the leader epoch chain.
        assertFalse(RemoteLogManager.isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                55,
                85,
                new TreeMap<Integer, Long>() {{
                    put(5, 55L);
                    put(6, 60L); // epoch 6 exists here but it is missing in leaderEpochToStartOffset
                    put(7, 70L);
                }}), logEndOffset, leaderEpochToStartOffset));

        // Test whether an epoch existing in remote segment does not exist in leader epoch chain.
        assertFalse(RemoteLogManager.isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                45,
                new TreeMap<Integer, Long>() {{
                    put(1, 15L);
                    put(2, 20L); // epoch 3 is missing here which exists in leaderEpochToStartOffset
                    put(4, 40L);
                }}), logEndOffset, leaderEpochToStartOffset));

        // Test a remote segment having larger end offset than the log end offset
        assertFalse(RemoteLogManager.isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                95, // larger than log end offset
                new TreeMap<Integer, Long>() {{
                    put(1, 15L);
                    put(2, 20L);
                }}), logEndOffset, leaderEpochToStartOffset));

        // Test whether a segment's first offset is earlier to the respective epoch's start offset
        assertFalse(RemoteLogManager.isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                5, // earlier to epoch 1's start offset
                25,
                new TreeMap<Integer, Long>() {{
                    put(1, 5L);
                    put(2, 20L);
                }}), logEndOffset, leaderEpochToStartOffset));

        // Test whether a segment's last offset is more than the respective epoch's end offset
        assertFalse(RemoteLogManager.isRemoteSegmentWithinLeaderEpochs(createRemoteLogSegmentMetadata(
                15,
                35, // more than epoch 2's end offset
                new TreeMap<Integer, Long>() {{
                    put(1, 15L);
                    put(2, 20L);
                }}), logEndOffset, leaderEpochToStartOffset));
    }

    @Test
    public void testCandidateLogSegmentsSkipsActiveSegment() {
        UnifiedLog log = mock(UnifiedLog.class);
        LogSegment segment1 = mock(LogSegment.class);
        LogSegment segment2 = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(segment1.baseOffset()).thenReturn(5L);
        when(segment2.baseOffset()).thenReturn(10L);
        when(activeSegment.baseOffset()).thenReturn(15L);

        when(log.logSegments(5L, Long.MAX_VALUE))
                .thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(segment1, segment2, activeSegment)));

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        List<RemoteLogManager.EnrichedLogSegment> expected =
                Arrays.asList(
                        new RemoteLogManager.EnrichedLogSegment(segment1, 10L),
                        new RemoteLogManager.EnrichedLogSegment(segment2, 15L)
                );
        List<RemoteLogManager.EnrichedLogSegment> actual = task.candidateLogSegments(log, 5L, 20L);
        assertEquals(expected, actual);
    }

    @Test
    public void testCandidateLogSegmentsSkipsSegmentsAfterLastStableOffset() {
        UnifiedLog log = mock(UnifiedLog.class);
        LogSegment segment1 = mock(LogSegment.class);
        LogSegment segment2 = mock(LogSegment.class);
        LogSegment segment3 = mock(LogSegment.class);
        LogSegment activeSegment = mock(LogSegment.class);

        when(segment1.baseOffset()).thenReturn(5L);
        when(segment2.baseOffset()).thenReturn(10L);
        when(segment3.baseOffset()).thenReturn(15L);
        when(activeSegment.baseOffset()).thenReturn(20L);

        when(log.logSegments(5L, Long.MAX_VALUE))
                .thenReturn(JavaConverters.collectionAsScalaIterable(Arrays.asList(segment1, segment2, segment3, activeSegment)));

        RemoteLogManager.RLMTask task = remoteLogManager.new RLMTask(leaderTopicIdPartition, 128);
        List<RemoteLogManager.EnrichedLogSegment> expected =
                Arrays.asList(
                        new RemoteLogManager.EnrichedLogSegment(segment1, 10L),
                        new RemoteLogManager.EnrichedLogSegment(segment2, 15L)
                );
        List<RemoteLogManager.EnrichedLogSegment> actual = task.candidateLogSegments(log, 5L, 15L);
        assertEquals(expected, actual);
    }

    @Test
    public void testStopPartitionsWithoutDeletion() throws RemoteStorageException {
        BiConsumer<TopicPartition, Throwable> errorHandler = (topicPartition, throwable) -> fail("shouldn't be called");
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(leaderTopicIdPartition.topicPartition());
        partitions.add(followerTopicIdPartition.topicPartition());
        remoteLogManager.onLeadershipChange(Collections.singleton(mockPartition(leaderTopicIdPartition)),
                Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);

        remoteLogManager.stopPartitions(partitions, false, errorHandler);
        verify(remoteLogMetadataManager, times(1)).onStopPartitions(any());
        verify(remoteStorageManager, times(0)).deleteLogSegmentData(any());
        verify(remoteLogMetadataManager, times(0)).updateRemoteLogSegmentMetadata(any());
    }

    @Test
    public void testStopPartitionsWithDeletion() throws RemoteStorageException {
        BiConsumer<TopicPartition, Throwable> errorHandler =
                (topicPartition, ex) -> fail("shouldn't be called: " + ex);
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(leaderTopicIdPartition.topicPartition());
        partitions.add(followerTopicIdPartition.topicPartition());
        remoteLogManager.onLeadershipChange(Collections.singleton(mockPartition(leaderTopicIdPartition)),
                Collections.singleton(mockPartition(followerTopicIdPartition)), topicIds);
        when(remoteLogMetadataManager.listRemoteLogSegments(eq(leaderTopicIdPartition)))
                .thenReturn(listRemoteLogSegmentMetadata(leaderTopicIdPartition, 5, 100, 1024).iterator());
        when(remoteLogMetadataManager.listRemoteLogSegments(eq(followerTopicIdPartition)))
                .thenReturn(listRemoteLogSegmentMetadata(followerTopicIdPartition, 3, 100, 1024).iterator());
        CompletableFuture<Void> dummyFuture = new CompletableFuture<>();
        dummyFuture.complete(null);
        when(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(any()))
                .thenReturn(dummyFuture);

        remoteLogManager.stopPartitions(partitions, true, errorHandler);
        verify(remoteLogMetadataManager, times(1)).onStopPartitions(any());
        verify(remoteStorageManager, times(8)).deleteLogSegmentData(any());
        verify(remoteLogMetadataManager, times(16)).updateRemoteLogSegmentMetadata(any());
    }

    private List<RemoteLogSegmentMetadata> listRemoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                        int segmentCount,
                                                                        int recordsPerSegment,
                                                                        int segmentSize) {
        List<RemoteLogSegmentMetadata> segmentMetadataList = new ArrayList<>();
        for (int idx = 0; idx < segmentCount; idx++) {
            long timestamp = time.milliseconds();
            long startOffset = (long) idx * recordsPerSegment;
            long endOffset = startOffset + recordsPerSegment - 1;
            Map<Integer, Long> segmentLeaderEpochs = truncateAndGetLeaderEpochs(totalEpochEntries, startOffset, endOffset);
            segmentMetadataList.add(new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition,
                    Uuid.randomUuid()), startOffset, endOffset, timestamp, brokerId, timestamp, segmentSize,
                    segmentLeaderEpochs));
        }
        return segmentMetadataList;
    }

    private Map<Integer, Long> truncateAndGetLeaderEpochs(List<EpochEntry> entries,
                                                          Long startOffset,
                                                          Long endOffset) {
        InMemoryLeaderEpochCheckpoint myCheckpoint = new InMemoryLeaderEpochCheckpoint();
        myCheckpoint.write(entries);
        LeaderEpochFileCache cache = new LeaderEpochFileCache(null, myCheckpoint);
        cache.truncateFromStart(startOffset);
        cache.truncateFromEnd(endOffset);
        return myCheckpoint.read().stream().collect(Collectors.toMap(e -> e.epoch, e -> e.startOffset));
    }

    private Partition mockPartition(TopicIdPartition topicIdPartition) {
        TopicPartition tp = topicIdPartition.topicPartition();
        Partition partition = mock(Partition.class);
        UnifiedLog log = mock(UnifiedLog.class);
        when(partition.topicPartition()).thenReturn(tp);
        when(partition.topic()).thenReturn(tp.topic());
        when(log.remoteLogEnabled()).thenReturn(true);
        when(partition.log()).thenReturn(Option.apply(log));
        return partition;
    }

    private RemoteLogManagerConfig createRLMConfig(Properties props) {
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true);
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, NoOpRemoteStorageManager.class.getName());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, NoOpRemoteLogMetadataManager.class.getName());
        props.put(DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + remoteLogStorageTestProp, remoteLogStorageTestVal);
        // adding configs with "remote log metadata manager config prefix"
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP, remoteLogMetadataTopicPartitionsNum);
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + remoteLogMetadataTestProp, remoteLogMetadataTestVal);
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + remoteLogMetadataCommonClientTestProp, remoteLogMetadataCommonClientTestVal);
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + remoteLogMetadataConsumerTestProp, remoteLogMetadataConsumerTestVal);
        props.put(DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + remoteLogMetadataProducerTestProp, remoteLogMetadataProducerTestVal);

        AbstractConfig config = new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props);
        return new RemoteLogManagerConfig(config);
    }

}
