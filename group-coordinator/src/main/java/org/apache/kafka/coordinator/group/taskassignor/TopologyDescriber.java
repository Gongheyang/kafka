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
package org.apache.kafka.coordinator.group.taskassignor;

/**
 * The subscribed topic describer is used by the {@link TaskAssignor} to obtain topic and task metadata of the groups topology.
 */
public interface TopologyDescriber {

    /**
     * The number of partitions for the given subtopology.
     *
     * @param subtopologyId String identifying the subtopology.
     * @return The number of tasks corresponding to the given subtopology ID, or -1 if the subtopology ID does not exist.
     */
    int numPartitions(String subtopologyId);

    /**
     * Whether the given subtopology is stateful.
     *
     * @param subtopologyId String identifying the subtopology.
     * @return true if the subtopology is stateful, false otherwise.
     */
    boolean isStateful(String subtopologyId);

}
