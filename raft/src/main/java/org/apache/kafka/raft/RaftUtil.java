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
package org.apache.kafka.raft;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.raft.internals.ReplicaKey;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static java.util.Collections.singletonList;

@SuppressWarnings("ClassDataAbstractionCoupling")
public class RaftUtil {

    public static ApiMessage errorResponse(ApiKeys apiKey, Errors error) {
        switch (apiKey) {
            case VOTE:
                return new VoteResponseData().setErrorCode(error.code());
            case BEGIN_QUORUM_EPOCH:
                return new BeginQuorumEpochResponseData().setErrorCode(error.code());
            case END_QUORUM_EPOCH:
                return new EndQuorumEpochResponseData().setErrorCode(error.code());
            case FETCH:
                return new FetchResponseData().setErrorCode(error.code());
            case FETCH_SNAPSHOT:
                return new FetchSnapshotResponseData().setErrorCode(error.code());
            default:
                throw new IllegalArgumentException("Received response for unexpected request type: " + apiKey);
        }
    }

    public static FetchRequestData singletonFetchRequest(
        TopicPartition topicPartition,
        Uuid topicId,
        Consumer<FetchRequestData.FetchPartition> partitionConsumer
    ) {
        FetchRequestData.FetchPartition fetchPartition =
            new FetchRequestData.FetchPartition()
                .setPartition(topicPartition.partition());
        partitionConsumer.accept(fetchPartition);

        FetchRequestData.FetchTopic fetchTopic =
            new FetchRequestData.FetchTopic()
                .setTopic(topicPartition.topic())
                .setTopicId(topicId)
                .setPartitions(singletonList(fetchPartition));

        return new FetchRequestData()
            .setTopics(singletonList(fetchTopic));
    }

    public static FetchResponseData singletonFetchResponse(
        ListenerName listenerName,
        short apiVersion,
        TopicPartition topicPartition,
        Uuid topicId,
        Errors topLevelError,
        int leaderId,
        Endpoints endpoints,
        Consumer<FetchResponseData.PartitionData> partitionConsumer
    ) {
        FetchResponseData.PartitionData fetchablePartition =
            new FetchResponseData.PartitionData();

        fetchablePartition.setPartitionIndex(topicPartition.partition());

        partitionConsumer.accept(fetchablePartition);

        FetchResponseData.FetchableTopicResponse fetchableTopic =
            new FetchResponseData.FetchableTopicResponse()
                .setTopic(topicPartition.topic())
                .setTopicId(topicId)
                .setPartitions(Collections.singletonList(fetchablePartition));

        FetchResponseData response = new FetchResponseData();

        if (apiVersion >= 17) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                FetchResponseData.NodeEndpointCollection nodeEndpoints = new FetchResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new FetchResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response
            .setErrorCode(topLevelError.code())
            .setResponses(Collections.singletonList(fetchableTopic));
    }

    public static VoteRequestData singletonVoteRequest(
        TopicPartition topicPartition,
        String clusterId,
        int candidateEpoch,
        ReplicaKey candidateKey,
        ReplicaKey voterKey,
        int lastEpoch,
        long lastEpochEndOffset
    ) {
        return new VoteRequestData()
            .setClusterId(clusterId)
            .setVoterId(voterKey.id())
            .setTopics(
                Collections.singletonList(
                    new VoteRequestData.TopicData()
                        .setTopicName(topicPartition.topic())
                        .setPartitions(
                            Collections.singletonList(
                                new VoteRequestData.PartitionData()
                                    .setPartitionIndex(topicPartition.partition())
                                    .setCandidateEpoch(candidateEpoch)
                                    .setCandidateId(candidateKey.id())
                                    .setCandidateDirectoryId(
                                        candidateKey
                                            .directoryId()
                                            .orElse(ReplicaKey.NO_DIRECTORY_ID)
                                    )
                                    .setVoterDirectoryId(
                                        voterKey
                                            .directoryId()
                                            .orElse(ReplicaKey.NO_DIRECTORY_ID)
                                    )
                                    .setLastOffsetEpoch(lastEpoch)
                                    .setLastOffset(lastEpochEndOffset)
                            )
                        )
                )
            );
    }

    public static VoteResponseData singletonVoteResponse(
        ListenerName listenerName,
        short apiVersion,
        Errors topLevelError,
        TopicPartition topicPartition,
        Errors partitionLevelError,
        int leaderEpoch,
        int leaderId,
        boolean voteGranted,
        Endpoints endpoints
    ) {
        VoteResponseData response = new VoteResponseData()
            .setErrorCode(topLevelError.code())
            .setTopics(Collections.singletonList(
                new VoteResponseData.TopicData()
                    .setTopicName(topicPartition.topic())
                    .setPartitions(Collections.singletonList(
                        new VoteResponseData.PartitionData()
                            .setErrorCode(partitionLevelError.code())
                            .setLeaderId(leaderId)
                            .setLeaderEpoch(leaderEpoch)
                            .setVoteGranted(voteGranted)))));

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                VoteResponseData.NodeEndpointCollection nodeEndpoints = new VoteResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new VoteResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }

    /**
     * Creates a FetchSnapshotResponseData with a single PartitionSnapshot for the topic partition.
     *
     * The partition index will already be populated when calling operator.
     *
     * @param topicPartition the topic partition to include
     * @param operator unary operator responsible for populating all of the appropriate fields
     * @return the created fetch snapshot response data
     */
    public static FetchSnapshotResponseData singletonFetchSnapshotResponse(
        ListenerName listenerName,
        short apiVersion,
        TopicPartition topicPartition,
        int leaderId,
        Endpoints endpoints,
        UnaryOperator<FetchSnapshotResponseData.PartitionSnapshot> operator
    ) {
        FetchSnapshotResponseData.PartitionSnapshot partitionSnapshot = operator.apply(
            new FetchSnapshotResponseData.PartitionSnapshot().setIndex(topicPartition.partition())
        );

        FetchSnapshotResponseData response = new FetchSnapshotResponseData()
            .setTopics(
                Collections.singletonList(
                    new FetchSnapshotResponseData.TopicSnapshot()
                        .setName(topicPartition.topic())
                        .setPartitions(Collections.singletonList(partitionSnapshot))
                )
            );

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                FetchSnapshotResponseData.NodeEndpointCollection nodeEndpoints =
                    new FetchSnapshotResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new FetchSnapshotResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }

    public static BeginQuorumEpochResponseData singletonBeginQuorumEpochResponse(
        ListenerName listenerName,
        short apiVersion,
        Errors topLevelError,
        TopicPartition topicPartition,
        Errors partitionLevelError,
        int leaderEpoch,
        int leaderId,
        Endpoints endpoints
    ) {
        BeginQuorumEpochResponseData response = new BeginQuorumEpochResponseData()
            .setErrorCode(topLevelError.code())
            .setTopics(
                Collections.singletonList(
                    new BeginQuorumEpochResponseData.TopicData()
                        .setTopicName(topicPartition.topic())
                        .setPartitions(
                            Collections.singletonList(
                                new BeginQuorumEpochResponseData.PartitionData()
                                    .setErrorCode(partitionLevelError.code())
                                    .setLeaderId(leaderId)
                                    .setLeaderEpoch(leaderEpoch)
                            )
                        )
                )
            );

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                BeginQuorumEpochResponseData.NodeEndpointCollection nodeEndpoints =
                    new BeginQuorumEpochResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new BeginQuorumEpochResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }

    public static EndQuorumEpochResponseData singletonEndQuorumEpochResponse(
        ListenerName listenerName,
        short apiVersion,
        Errors topLevelError,
        TopicPartition topicPartition,
        Errors partitionLevelError,
        int leaderEpoch,
        int leaderId,
        Endpoints endpoints
    ) {
        EndQuorumEpochResponseData response = new EndQuorumEpochResponseData()
                   .setErrorCode(topLevelError.code())
                   .setTopics(Collections.singletonList(
                       new EndQuorumEpochResponseData.TopicData()
                           .setTopicName(topicPartition.topic())
                           .setPartitions(Collections.singletonList(
                               new EndQuorumEpochResponseData.PartitionData()
                                   .setErrorCode(partitionLevelError.code())
                                   .setLeaderId(leaderId)
                                   .setLeaderEpoch(leaderEpoch)
                           )))
                   );

        if (apiVersion >= 1) {
            Optional<InetSocketAddress> address = endpoints.address(listenerName);
            if (address.isPresent() && leaderId >= 0) {
                // Populate the node endpoints
                EndQuorumEpochResponseData.NodeEndpointCollection nodeEndpoints =
                    new EndQuorumEpochResponseData.NodeEndpointCollection(1);
                nodeEndpoints.add(
                    new EndQuorumEpochResponseData.NodeEndpoint()
                        .setNodeId(leaderId)
                        .setHost(address.get().getHostString())
                        .setPort(address.get().getPort())
                );
                response.setNodeEndpoints(nodeEndpoints);
            }
        }

        return response;
    }

    static Optional<ReplicaKey> voteRequestVoterId(
        VoteRequestData request,
        VoteRequestData.PartitionData partition
    ) {
        if (request.voterId() < 0) {
            return Optional.empty();
        } else {
            return Optional.of(ReplicaKey.of(request.voterId(), partition.voterDirectoryId()));
        }
    }

    static boolean hasValidTopicPartition(FetchRequestData data, TopicPartition topicPartition, Uuid topicId) {
        return data.topics().size() == 1 &&
            data.topics().get(0).topicId().equals(topicId) &&
            data.topics().get(0).partitions().size() == 1 &&
            data.topics().get(0).partitions().get(0).partition() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(FetchResponseData data, TopicPartition topicPartition, Uuid topicId) {
        return data.responses().size() == 1 &&
            data.responses().get(0).topicId().equals(topicId) &&
            data.responses().get(0).partitions().size() == 1 &&
            data.responses().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(VoteResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(VoteRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(BeginQuorumEpochRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(BeginQuorumEpochResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(EndQuorumEpochRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(EndQuorumEpochResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(DescribeQuorumRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }
}
