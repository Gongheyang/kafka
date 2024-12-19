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
package org.apache.kafka.admin;

import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.server.common.AdminOperationException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AdminRackAwareTest {
    public static class ReplicaDistributions {
        private final Map<Integer, List<String>> partitionRacks;
        private final Map<Integer, Integer> brokerLeaderCount;
        private final Map<Integer, Integer> brokerReplicasCount;

        public ReplicaDistributions(Map<Integer, List<String>> partitionRacks,
                                    Map<Integer, Integer> brokerLeaderCount,
                                    Map<Integer, Integer> brokerReplicasCount) {
            this.partitionRacks = partitionRacks;
            this.brokerLeaderCount = brokerLeaderCount;
            this.brokerReplicasCount = brokerReplicasCount;
        }

        public Map<Integer, List<String>> getPartitionRacks() {
            return partitionRacks;
        }

        public Map<Integer, Integer> getBrokerLeaderCount() {
            return brokerLeaderCount;
        }

        public Map<Integer, Integer> getBrokerReplicasCount() {
            return brokerReplicasCount;
        }
    }

    private static Collection<BrokerMetadata> toBrokerMetadata(Map<Integer, String> rackMap) {
        List<BrokerMetadata> brokerMetadatas = rackMap.entrySet().stream()
                .map(entry -> new BrokerMetadata(entry.getKey(), Optional.ofNullable(entry.getValue())))
                .sorted(Comparator.comparingInt(b -> b.id)).collect(Collectors.toList());

        return brokerMetadatas;
    }

    private static ReplicaDistributions getReplicaDistribution(Map<Integer, List<Integer>> assignment, Map<Integer, String> brokerRackMapping) {
        Map<Integer, Integer> leaderCount = new HashMap<>();
        Map<Integer, Integer> partitionCount = new HashMap<>();
        Map<Integer, List<String>> partitionRackMap = new HashMap<>();

        for (Map.Entry<Integer, List<Integer>> entry : assignment.entrySet()) {
            Integer partitionId = entry.getKey();
            List<Integer> replicaList = entry.getValue();

            // The leader is the first broker in the list
            leaderCount.merge(replicaList.get(0), 1, Integer::sum);

            for (Integer brokerId : replicaList) {
                partitionCount.merge(brokerId, 1, Integer::sum);
                // Add the rack to the list of racks for this partition
                partitionRackMap.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(brokerRackMapping.get(brokerId));
            }
        }

        return new ReplicaDistributions(partitionRackMap, leaderCount, partitionCount);
    }

    public static void assertReplicaDistribution(
            Map<Integer, List<Integer>> assignment,
            Map<Integer, String> brokerRackMapping,
            int numBrokers,
            int numPartitions,
            int replicationFactor,
            boolean verifyRackAware,
            boolean verifyLeaderDistribution,
            boolean verifyReplicasDistribution) {

        for (Map.Entry<Integer, List<Integer>> entry : assignment.entrySet()) {
            List<Integer> brokerList = entry.getValue();
            Set<Integer> brokerSet = new HashSet<>(brokerList);
            assertEquals(brokerSet.size(), brokerList.size());
        }

        ReplicaDistributions distribution = getReplicaDistribution(assignment, brokerRackMapping);

        // Verify Rack-Awareness
        if (verifyRackAware) {
            Map<Integer, List<String>> partitionRackMap = distribution.getPartitionRacks();
            List<Integer> distinctRackSizes = partitionRackMap.values().stream()
                    .map(rackList -> (int) rackList.stream().distinct().count())
                    .collect(Collectors.toList());

            List<Integer> expectedRackSizes = Collections.nCopies(numPartitions, replicationFactor);
            assertEquals(expectedRackSizes, distinctRackSizes);
        }

        // Verify Leader Distribution
        if (verifyLeaderDistribution) {
            Map<Integer, Integer> leaderCount = distribution.getBrokerLeaderCount();
            int leaderCountPerBroker = numPartitions / numBrokers;
            List<Integer> expectedLeaderCounts = Collections.nCopies(numBrokers, leaderCountPerBroker);
            List<Integer> actualLeaderCounts = new ArrayList<>(leaderCount.values());
            assertEquals(expectedLeaderCounts, actualLeaderCounts);
        }

        // Verify Replicas Distribution
        if (verifyReplicasDistribution) {
            Map<Integer, Integer> replicasCount = distribution.getBrokerReplicasCount();
            int numReplicasPerBroker = (numPartitions * replicationFactor) / numBrokers;
            List<Integer> expectedReplicasCounts = Collections.nCopies(numBrokers, numReplicasPerBroker);
            List<Integer> actualReplicasCounts = new ArrayList<>(replicasCount.values());
            assertEquals(expectedReplicasCounts, actualReplicasCounts);
        }
    }

    @Test
    public void testGetRackAlternatedBrokerListAndAssignReplicasToBrokers() {
        Map<Integer, String> rackMap = Map.of(
                0, "rack1",
                1, "rack3",
                2, "rack3",
                3, "rack2",
                4, "rack2",
                5, "rack1"
        );

        List<Integer> newList = AdminUtils.getRackAlternatedBrokerList(rackMap);
        assertEquals(Arrays.asList(0, 3, 1, 5, 4, 2), newList);

        Map<Integer, String> anotherRackMap = new HashMap<>(rackMap);
        anotherRackMap.remove(5);
        List<Integer> anotherList = AdminUtils.getRackAlternatedBrokerList(anotherRackMap);
        assertEquals(Arrays.asList(0, 3, 1, 4, 2), anotherList);

        Map<Integer, List<Integer>> assignment = AdminUtils.
                assignReplicasToBrokers(toBrokerMetadata(rackMap), 7, 3, 0, 0);


        Map<Integer, List<Integer>> expected = Map.of(
                0, Arrays.asList(0, 3, 1),
                1, Arrays.asList(3, 1, 5),
                2, Arrays.asList(1, 5, 4),
                3, Arrays.asList(5, 4, 2),
                4, Arrays.asList(4, 2, 0),
                5, Arrays.asList(2, 0, 3),
                6, Arrays.asList(0, 4, 2)
        );

        assertEquals(expected, assignment);
    }

    @Test
    public void testAssignmentWithRackAware() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack2",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack1"
        );

        int numPartitions = 6;
        int replicationFactor = 3;
        Map<Integer, List<Integer>> assignment = AdminUtils.
                assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor, 2, 0);

        assertReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(),
                numPartitions, replicationFactor, true, true, true);
    }

    @Test
    public void testAssignmentWithRackAwareWithRandomStartIndex() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack2",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack1"
        );

        int numPartitions = 6;
        int replicationFactor = 3;
        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor);

        assertReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(),
                numPartitions, replicationFactor, true, true, true);
    }

    @Test
    public void testAssignmentWithRackAwareWithUnevenReplicas() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack2",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack1"
        );

        int numPartitions = 13;
        int replicationFactor = 3;
        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor, 0, 0);

        assertReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(),
                numPartitions, replicationFactor, true, false, false);
    }

    @Test
    public void testAssignmentWithRackAwareWithUnevenRacks() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack1",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack1"
        );

        int numPartitions = 12;
        int replicationFactor = 3;
        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor);

        assertReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(),
                numPartitions, replicationFactor, true, true, false);
    }

    @Test
    public void testAssignmentWith2ReplicasRackAware() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack2",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack1"
        );

        int numPartitions = 12;
        int replicationFactor = 2;
        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor);

        assertReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(),
                numPartitions, replicationFactor, true, true, true);
    }

    @Test
    public void testRackAwareExpansion() {
        Map<Integer, String> brokerRackMapping = Map.of(
                6, "rack1",
                7, "rack2",
                8, "rack2",
                9, "rack3",
                10, "rack3",
                11, "rack1"
        );

        int numPartitions = 12;
        int replicationFactor = 2;
        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor, -1, 12);

        assertReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(),
                numPartitions, replicationFactor, true, true, true);
    }

    @Test
    public void testAssignmentWith2ReplicasRackAwareWith6Partitions() {
        // Define the brokerRackMapping in Java
        Map<Integer, String> brokerRackMapping = Map.of(
                6, "rack1",
                7, "rack2",
                8, "rack2",
                9, "rack3",
                10, "rack3",
                11, "rack1"
        );

        int numPartitions = 6;
        int replicationFactor = 2;

        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor);

        assertReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(),
                numPartitions, replicationFactor, true, true, true);
    }

    @Test
    public void testAssignmentWith2ReplicasRackAwareWith6PartitionsAnd3Brokers() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack2",
                4, "rack3"
        );

        int numPartitions = 3;
        int replicationFactor = 2;

        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor);

        assertReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(),
                numPartitions, replicationFactor, true, true, true);
    }

    @Test
    public void testLargeNumberPartitionsAssignment() {
        Map<Integer, String> brokerRackMapping = new HashMap<>();
        brokerRackMapping.put(0, "rack1");
        brokerRackMapping.put(1, "rack2");
        brokerRackMapping.put(2, "rack2");
        brokerRackMapping.put(3, "rack3");
        brokerRackMapping.put(4, "rack3");
        brokerRackMapping.put(5, "rack1");
        brokerRackMapping.put(6, "rack1");
        brokerRackMapping.put(7, "rack2");
        brokerRackMapping.put(8, "rack2");
        brokerRackMapping.put(9, "rack3");
        brokerRackMapping.put(10, "rack1");
        brokerRackMapping.put(11, "rack3");

        int numPartitions = 96;
        int replicationFactor = 3;

        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor);

        assertReplicaDistribution(assignment, brokerRackMapping, brokerRackMapping.size(),
                numPartitions, replicationFactor, true, true, true);
    }

    @Test
    public void testMoreReplicasThanRacks() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack2",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack2"
        );

        int numPartitions = 6;
        int replicationFactor = 5;

        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor);

        assignment.values().forEach(replicas -> {
            assertEquals(replicationFactor, replicas.size());
        });

        ReplicaDistributions distribution = getReplicaDistribution(assignment, brokerRackMapping);

        // Verify that each partition is spread across exactly 3 unique racks
        for (int partition = 0; partition < numPartitions; partition++) {
            List<String> racks = distribution.getPartitionRacks().get(partition);
            long distinctRackCount = racks.stream().distinct().count();
            assertEquals(3, distinctRackCount);
        }

    }

    @Test
    public void testLessReplicasThanRacks() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack2",
                2, "rack2",
                3, "rack3",
                4, "rack3",
                5, "rack2"
        );

        int numPartitions = 6;
        int replicationFactor = 2;

        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor);

        // Assert that each partition is assigned to the correct number of replicas
        assignment.values().forEach(replicas -> {
            assertEquals(replicationFactor, replicas.size());
        });

        // Get the replica distribution
        ReplicaDistributions distribution = getReplicaDistribution(assignment, brokerRackMapping);

        // Verify that each partition is spread across exactly 2 unique racks
        for (int partition = 0; partition <= 5; partition++) {
            List<String> racks = distribution.getPartitionRacks().get(partition);
            long distinctRackCount = racks.stream().distinct().count();
            assertEquals(2, distinctRackCount);
        }

    }

    @Test
    public void testSingleRack() {
        Map<Integer, String> brokerRackMapping = Map.of(
                0, "rack1",
                1, "rack1",
                2, "rack1",
                3, "rack1",
                4, "rack1",
                5, "rack1"
        );

        int numPartitions = 6;
        int replicationFactor = 3;

        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(toBrokerMetadata(brokerRackMapping), numPartitions, replicationFactor);

        assignment.values().forEach(replicas -> {
            assertEquals(replicationFactor, replicas.size());
        });

        ReplicaDistributions distribution = getReplicaDistribution(assignment, brokerRackMapping);

        for (int partition = 0; partition < numPartitions; partition++) {
            List<String> racks = distribution.getPartitionRacks().get(partition);
            long distinctRackCount = racks.stream().distinct().count();
            assertEquals(1, distinctRackCount);
        }

        brokerRackMapping.keySet().forEach(broker -> {
            assertEquals(1, distribution.getBrokerLeaderCount().getOrDefault(broker, 0));
        });
    }

    @Test
    public void testSkipBrokerWithReplicaAlreadyAssigned() {
        Map<Integer, String> rackInfo = Map.of(
                0, "a",
                1, "b",
                2, "c",
                3, "a",
                4, "a"
        );

        List<Integer> brokerList = IntStream.rangeClosed(0, 4).boxed().collect(Collectors.toList());

        int numPartitions = 6;
        int replicationFactor = 4;

        List<BrokerMetadata> brokerMetadatas = new ArrayList<>(toBrokerMetadata(rackInfo));
        List<Integer> brokerIds = brokerMetadatas.stream().map(b -> b.id).collect(Collectors.toList());
        assertEquals(brokerList, brokerIds);

        Map<Integer, List<Integer>> assignment =
                AdminUtils.assignReplicasToBrokers(brokerMetadatas, numPartitions, replicationFactor, 2, -1);

        assertReplicaDistribution(assignment, rackInfo, 5, numPartitions, replicationFactor, false, false, false);

    }

    @Test
    public void testReplicaAssignment() {
        // Create broker metadata for brokers 0 to 4
        List<BrokerMetadata> brokerMetadatas = IntStream.rangeClosed(0, 4)
                .mapToObj(brokerId -> new BrokerMetadata(brokerId, Optional.empty()))
                .collect(Collectors.toList());

        // Test 0 replication factor
        assertThrows(InvalidReplicationFactorException.class,
                () -> AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, 0));


        assertThrows(InvalidReplicationFactorException.class,
                () -> AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, 6));

        Map<Integer, List<Integer>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(0, Arrays.asList(0, 1, 2));
        expectedAssignment.put(1, Arrays.asList(1, 2, 3));
        expectedAssignment.put(2, Arrays.asList(2, 3, 4));
        expectedAssignment.put(3, Arrays.asList(3, 4, 0));
        expectedAssignment.put(4, Arrays.asList(4, 0, 1));
        expectedAssignment.put(5, Arrays.asList(0, 2, 3));
        expectedAssignment.put(6, Arrays.asList(1, 3, 4));
        expectedAssignment.put(7, Arrays.asList(2, 4, 0));
        expectedAssignment.put(8, Arrays.asList(3, 0, 1));
        expectedAssignment.put(9, Arrays.asList(4, 1, 2));

        // Get the actual assignment
        Map<Integer, List<Integer>> actualAssignment =
                AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, 3, 0, -1);

        // Assert that the expected assignment matches the actual assignment
        assertEquals(expectedAssignment, actualAssignment);
    }

    @Test
    public void testAssignReplicasToBrokersWithInvalidParameters() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack3");
        rackMap.put(2, "rack3");
        rackMap.put(3, "rack2");
        rackMap.put(4, null);

        Collection<BrokerMetadata> brokerMetadatas = toBrokerMetadata(rackMap);

        // Test 0 partitions
        assertThrows(InvalidPartitionsException.class,
                () -> AdminUtils.assignReplicasToBrokers(brokerMetadatas, 0, 0, -1, -1));

        // Test 0 replication factor
        assertThrows(InvalidReplicationFactorException.class,
                () -> AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, 0, -1, -1));

        // Test wrong replication factor (greater than available brokers)
        assertThrows(InvalidReplicationFactorException.class,
                () -> AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, brokerMetadatas.size() + 1, -1, -1));

        // Test invalid brokerMetadatas
        assertThrows(AdminOperationException.class,
                () -> AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, brokerMetadatas.size(), -1, -1));
    }
}
