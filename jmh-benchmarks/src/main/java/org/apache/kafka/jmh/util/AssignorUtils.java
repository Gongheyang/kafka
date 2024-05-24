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
package org.apache.kafka.jmh.util;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.MemberAssignment;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AssignorUtils {
    /**
     * Generate a reverse look up map of partition to member target assignments from the given member spec.
     *
     * @param groupAssignment       The group assignment.
     * @return Map of topic partition to member assignments.
     */
    public static Map<Uuid, Map<Integer, String>> invertedTargetAssignment(
        GroupAssignment groupAssignment
    ) {
        Map<Uuid, Map<Integer, String>> invertedTargetAssignment = new HashMap<>();
        for (Map.Entry<String, MemberAssignment> memberEntry : groupAssignment.members().entrySet()) {
            String memberId = memberEntry.getKey();
            Map<Uuid, Set<Integer>> topicsAndPartitions = memberEntry.getValue().targetPartitions();

            for (Map.Entry<Uuid, Set<Integer>> topicEntry : topicsAndPartitions.entrySet()) {
                Uuid topicId = topicEntry.getKey();
                Set<Integer> partitions = topicEntry.getValue();

                Map<Integer, String> partitionMap = invertedTargetAssignment.computeIfAbsent(topicId, k -> new HashMap<>());

                for (Integer partitionId : partitions) {
                    partitionMap.put(partitionId, memberId);
                }
            }
        }
        return invertedTargetAssignment;
    }
}
