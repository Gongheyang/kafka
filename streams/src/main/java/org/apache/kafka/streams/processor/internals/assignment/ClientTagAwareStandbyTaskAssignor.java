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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.streams.processor.internals.assignment.StandbyTaskAssignmentUtils.computeTasksToRemainingStandbys;

/**
 * Distributes standby tasks over different tag dimensions.
 * Only tags specified via {@link AssignmentConfigs#rackAwareAssignmentTags} are taken into account.
 * Standby task distribution is on a best-effort basis. For example, if there are not enough clients available
 * on different tag dimensions compared to an active and corresponding standby task,
 * in that case, the algorithm will fall back to distributing tasks on least-loaded clients.
 */
class ClientTagAwareStandbyTaskAssignor implements StandbyTaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(ClientTagAwareStandbyTaskAssignor.class);

    @Override
    public boolean assign(final Map<UUID, ClientState> clients,
                          final Set<TaskId> allTaskIds,
                          final Set<TaskId> statefulTaskIds,
                          final AssignorConfiguration.AssignmentConfigs configs) {
        final int numStandbyReplicas = configs.numStandbyReplicas;
        final Set<String> rackAwareAssignmentTags = new HashSet<>(configs.rackAwareAssignmentTags);
        final Map<TaskId, UUID> statefulTasksWithClients = new HashMap<>();

        statefulTaskIds.forEach(statefulTaskId -> clients.forEach((uuid, clientState) -> {
            if (clientState.activeTasks().contains(statefulTaskId)) {
                statefulTasksWithClients.put(statefulTaskId, uuid);
            }
        }));

        final Map<TaskId, Integer> tasksToRemainingStandbys = computeTasksToRemainingStandbys(
            numStandbyReplicas,
            allTaskIds
        );

        final Map<String, Set<String>> tagKeyToTagValues = new HashMap<>();
        final Map<String, Set<UUID>> tagValueToClients = new HashMap<>();

        fillClientsTagStatistics(clients, tagValueToClients, tagKeyToTagValues);

        statefulTasksWithClients.forEach((taskId, clientId) -> assignStandbyTasksForActiveTask(
            numStandbyReplicas,
            taskId,
            clientId,
            rackAwareAssignmentTags,
            clients,
            tasksToRemainingStandbys,
            tagKeyToTagValues,
            tagValueToClients
        ));

        return true;
    }

    @Override
    public boolean isAllowedTaskMovement(final ClientState source, final ClientState destination) {
        final Map<String, String> sourceClientTags = source.clientTags();
        final Map<String, String> destinationClientTags = destination.clientTags();

        for (final Entry<String, String> sourceClientTagEntry : sourceClientTags.entrySet()) {
            if (!sourceClientTagEntry.getValue().equals(destinationClientTags.get(sourceClientTagEntry.getKey()))) {
                return false;
            }
        }

        return true;
    }

    private static void fillClientsTagStatistics(final Map<UUID, ClientState> clientStates,
                                                 final Map<String, Set<UUID>> tagValueToClients,
                                                 final Map<String, Set<String>> tagKeyToTagValues) {
        for (final Entry<UUID, ClientState> clientStateEntry : clientStates.entrySet()) {
            final UUID clientId = clientStateEntry.getKey();
            final ClientState clientState = clientStateEntry.getValue();

            clientState.clientTags().forEach((tagKey, tagValue) -> {
                tagKeyToTagValues.computeIfAbsent(tagKey, ignored -> new HashSet<>()).add(tagValue);
                tagValueToClients.computeIfAbsent(tagValue, ignored -> new HashSet<>()).add(clientId);
            });
        }
    }

    private static void assignStandbyTasksForActiveTask(final int numStandbyReplicas,
                                                        final TaskId activeTaskId,
                                                        final UUID activeTaskClient,
                                                        final Set<String> rackAwareAssignmentTags,
                                                        final Map<UUID, ClientState> clientStates,
                                                        final Map<TaskId, Integer> tasksToRemainingStandbys,
                                                        final Map<String, Set<String>> tagKeyToTagValues,
                                                        final Map<String, Set<UUID>> tagValueToClients) {
        final ConstrainedPrioritySet standbyTaskClientsByTaskLoad = new ConstrainedPrioritySet(
            (client, t) -> !clientStates.get(client).hasAssignedTask(t),
            client -> clientStates.get(client).assignedTaskLoad()
        );

        final Set<UUID> usedClients = new HashSet<>();

        standbyTaskClientsByTaskLoad.offerAll(clientStates.keySet());

        int numRemainingStandbys = tasksToRemainingStandbys.get(activeTaskId);

        usedClients.add(activeTaskClient);

        while (numRemainingStandbys > 0) {
            final Set<UUID> clientsOnAlreadyUsedTagDimensions = findClientsOnUsedTagDimensions(
                usedClients,
                rackAwareAssignmentTags,
                clientStates,
                tagValueToClients,
                tagKeyToTagValues
            );

            final UUID polledClient = standbyTaskClientsByTaskLoad.poll(
                activeTaskId, uuid -> !clientsOnAlreadyUsedTagDimensions.contains(uuid)
            );

            if (polledClient == null) {
                break;
            }

            final ClientState standbyTaskClient = clientStates.get(polledClient);

            standbyTaskClient.assignStandby(activeTaskId);

            usedClients.add(polledClient);

            standbyTaskClientsByTaskLoad.offerAll(clientsOnAlreadyUsedTagDimensions);
            numRemainingStandbys--;
        }

        if (numRemainingStandbys > 0) {
            log.warn("Unable to assign {} of {} standby tasks for task [{}] with client tags [{}]. " +
                     "There is not enough available capacity. You should " +
                     "increase the number of application instances " +
                     "on different client tag dimensions " +
                     "to maintain the requested number of standby replicas. " +
                     "Rack awareness is configured with [{}] tags.",
                     numRemainingStandbys, numStandbyReplicas, activeTaskId,
                     clientStates.get(activeTaskClient).clientTags(), rackAwareAssignmentTags);
        }
    }

    private static Set<UUID> findClientsOnUsedTagDimensions(final Set<UUID> usedClients,
                                                            final Set<String> rackAwareAssignmentTags,
                                                            final Map<UUID, ClientState> clientStates,
                                                            final Map<String, Set<UUID>> clientsPerTagValue,
                                                            final Map<String, Set<String>> tagKeyToTagValuesMapping) {
        final Set<UUID> filteredClients = new HashSet<>();

        for (final UUID usedClientId : usedClients) {
            final Map<String, String> usedClientTags = clientStates.get(usedClientId).clientTags();

            for (final Entry<String, String> usedClientTagEntry : usedClientTags.entrySet()) {
                final String tagKey = usedClientTagEntry.getKey();

                if (!rackAwareAssignmentTags.contains(tagKey)) {
                    continue;
                }

                final Set<String> allTagValues = tagKeyToTagValuesMapping.get(tagKey);
                final String tagValue = usedClientTagEntry.getValue();

                // If we have used more clients than all the tag's unique values,
                // we can't filter out clients located on that tag.
                if (allTagValues.size() <= usedClients.size()) {
                    continue;
                }

                final Set<UUID> clientsOnUsedTagValue = clientsPerTagValue.get(tagValue);
                filteredClients.addAll(clientsOnUsedTagValue);
            }
        }

        return filteredClients;
    }
}