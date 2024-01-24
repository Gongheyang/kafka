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
package org.apache.kafka.clients.admin.internals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.ConsumerGroupDescribeResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

public class DescribeConsumerGroupsHandler implements AdminApiHandler<CoordinatorKey, ConsumerGroupDescription> {

    private final boolean includeAuthorizedOperations;
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;
    private final Map<String, Boolean> useClassicGroupApi;

    public DescribeConsumerGroupsHandler(
        boolean includeAuthorizedOperations,
        LogContext logContext
    ) {
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        this.log = logContext.logger(DescribeConsumerGroupsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
        this.useClassicGroupApi = new HashMap<>();
    }

    private static Set<CoordinatorKey> buildKeySet(Collection<String> groupIds) {
        return groupIds.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, ConsumerGroupDescription> newFuture(
        Collection<String> groupIds
    ) {
        return AdminApiFuture.forKeys(buildKeySet(groupIds));
    }

    @Override
    public String apiName() {
        return "describeConsumerGroups";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    @Override
    public Collection<RequestAndKeys<CoordinatorKey>> buildRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        Set<CoordinatorKey> newConsumerGroups = new HashSet<>();
        Set<CoordinatorKey> oldConsumerGroups = new HashSet<>();

        keys.forEach(key -> {
            if (key.type != FindCoordinatorRequest.CoordinatorType.GROUP) {
                throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `DescribeGroups` request");
            }

            // Be default, we always try with using the new consumer group
            // describe API. If it fails, we fail back to using the classic
            // group API.
            if (useClassicGroupApi.getOrDefault(key.idValue, false)) {
                oldConsumerGroups.add(key);
            } else {
                newConsumerGroups.add(key);
            }
        });

        List<RequestAndKeys<CoordinatorKey>> requests = new ArrayList<>();
        if (!newConsumerGroups.isEmpty()) {
            ConsumerGroupDescribeRequestData data = new ConsumerGroupDescribeRequestData()
                .setGroupIds(newConsumerGroups.stream().map(key -> key.idValue).collect(Collectors.toList()))
                .setIncludeAuthorizedOperations(includeAuthorizedOperations);
            requests.add(new RequestAndKeys<>(new ConsumerGroupDescribeRequest.Builder(data, true), newConsumerGroups));
        }

        if (!oldConsumerGroups.isEmpty()) {
            DescribeGroupsRequestData data = new DescribeGroupsRequestData()
                .setGroups(oldConsumerGroups.stream().map(key -> key.idValue).collect(Collectors.toList()))
                .setIncludeAuthorizedOperations(includeAuthorizedOperations);
            requests.add(new RequestAndKeys<>(new DescribeGroupsRequest.Builder(data), oldConsumerGroups));
        }

        return requests;
    }

    @Override
    public ApiResult<CoordinatorKey, ConsumerGroupDescription> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        final Map<CoordinatorKey, ConsumerGroupDescription> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

        if (abstractResponse instanceof DescribeGroupsResponse) {
            return handledClassicGroupResponse(
                coordinator,
                completed,
                failed,
                groupsToUnmap,
                (DescribeGroupsResponse) abstractResponse
            );
        } else if (abstractResponse instanceof ConsumerGroupDescribeResponse) {
            return handledConsumerGroupResponse(
                coordinator,
                completed,
                failed,
                groupsToUnmap,
                (ConsumerGroupDescribeResponse) abstractResponse
            );
        } else {
            throw new IllegalArgumentException("Received an unexpected response type.");
        }
    }

    private ApiResult<CoordinatorKey, ConsumerGroupDescription> handledConsumerGroupResponse(
        Node coordinator,
        Map<CoordinatorKey, ConsumerGroupDescription> completed,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        ConsumerGroupDescribeResponse response
    ) {
        for (ConsumerGroupDescribeResponseData.DescribedGroup describedGroup : response.data().groups()) {
            final CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
            final Errors error = Errors.forCode(describedGroup.errorCode());
            if (error != Errors.NONE) {
                handleError(
                    groupIdKey,
                    error,
                    describedGroup.errorMessage(),
                    failed,
                    groupsToUnmap,
                    true
                );
                continue;
            }

            final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());
            final List<MemberDescription> memberDescriptions = new ArrayList<>(describedGroup.members().size());

            describedGroup.members().forEach(groupMember -> {
                final Set<TopicPartition> partitions = groupMember.assignment().topicPartitions().stream().flatMap(topic ->
                    topic.partitions().stream().map(partition ->
                        new TopicPartition(topic.topicName(), partition)
                    )
                ).collect(Collectors.toSet());

                memberDescriptions.add(new MemberDescription(
                    groupMember.memberId(),
                    Optional.ofNullable(groupMember.instanceId()),
                    groupMember.clientId(),
                    groupMember.clientHost(),
                    new MemberAssignment(partitions)));
            });

            final ConsumerGroupDescription consumerGroupDescription =
                new ConsumerGroupDescription(
                    groupIdKey.idValue,
                    false,
                    memberDescriptions,
                    describedGroup.assignorName(),
                    ConsumerGroupState.parse(describedGroup.groupState()),
                    coordinator,
                    authorizedOperations
                );
            completed.put(groupIdKey, consumerGroupDescription);
        }

        return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
    }

    private ApiResult<CoordinatorKey, ConsumerGroupDescription> handledClassicGroupResponse(
        Node coordinator,
        Map<CoordinatorKey, ConsumerGroupDescription> completed,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        DescribeGroupsResponse response
    ) {
        for (DescribedGroup describedGroup : response.data().groups()) {
            CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
            Errors error = Errors.forCode(describedGroup.errorCode());
            if (error != Errors.NONE) {
                handleError(
                    groupIdKey,
                    error,
                    null,
                    failed,
                    groupsToUnmap,
                    false
                );
                continue;
            }
            final String protocolType = describedGroup.protocolType();
            if (protocolType.equals(ConsumerProtocol.PROTOCOL_TYPE) || protocolType.isEmpty()) {
                final List<DescribedGroupMember> members = describedGroup.members();
                final List<MemberDescription> memberDescriptions = new ArrayList<>(members.size());
                final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());
                for (DescribedGroupMember groupMember : members) {
                    Set<TopicPartition> partitions = Collections.emptySet();
                    if (groupMember.memberAssignment().length > 0) {
                        final Assignment assignment = ConsumerProtocol.
                            deserializeAssignment(ByteBuffer.wrap(groupMember.memberAssignment()));
                        partitions = new HashSet<>(assignment.partitions());
                    }
                    memberDescriptions.add(new MemberDescription(
                        groupMember.memberId(),
                        Optional.ofNullable(groupMember.groupInstanceId()),
                        groupMember.clientId(),
                        groupMember.clientHost(),
                        new MemberAssignment(partitions)));
                }
                final ConsumerGroupDescription consumerGroupDescription =
                    new ConsumerGroupDescription(groupIdKey.idValue, protocolType.isEmpty(),
                        memberDescriptions,
                        describedGroup.protocolData(),
                        ConsumerGroupState.parse(describedGroup.groupState()),
                        coordinator,
                        authorizedOperations);
                completed.put(groupIdKey, consumerGroupDescription);
            } else {
                failed.put(groupIdKey, new IllegalArgumentException(
                    String.format("GroupId %s is not a consumer group (%s).",
                        groupIdKey.idValue, protocolType)));
            }
        }

        return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
    }

    private void handleError(
        CoordinatorKey groupId,
        Errors error,
        String errorMsg,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        boolean isConsumerGroupResponse
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                log.debug("`DescribeGroups` request for group id {} failed due to error {}.", groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`DescribeGroups` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry.", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`DescribeGroups` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry.", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            case GROUP_ID_NOT_FOUND:
                if (isConsumerGroupResponse) {
                    log.debug("`DescribeGroups` request for group id {} failed because the group is not " +
                        "a new consumer group. Will retry with old API.", groupId.idValue);
                    useClassicGroupApi.put(groupId.idValue, true);
                } else {
                    log.error("`DescribeGroups` request for group id {} because the group does not exist.", groupId.idValue);
                    failed.put(groupId, error.exception(errorMsg));
                }
                break;

            default:
                log.error("`DescribeGroups` request for group id {} failed due to unexpected error {}.", groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
        }
    }

    private Set<AclOperation> validAclOperations(final int authorizedOperations) {
        if (authorizedOperations == MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED) {
            return null;
        }
        return Utils.from32BitField(authorizedOperations)
            .stream()
            .map(AclOperation::fromCode)
            .filter(operation -> operation != AclOperation.UNKNOWN
                && operation != AclOperation.ALL
                && operation != AclOperation.ANY)
            .collect(Collectors.toSet());
    }

}
