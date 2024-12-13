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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A description of the assignments of a specific share group member.
 */
@InterfaceStability.Evolving
public class ShareMemberAssignment {
    private final Set<TopicPartition> topicPartitions;

    /**
     * Creates an instance with the specified parameters.
     *
     * @param topicPartitions List of topic partitions
     */
    public ShareMemberAssignment(Set<TopicPartition> topicPartitions) {
        this.topicPartitions = topicPartitions == null ? Collections.emptySet() : Set.copyOf(topicPartitions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShareMemberAssignment that = (ShareMemberAssignment) o;

        return Objects.equals(topicPartitions, that.topicPartitions);
    }

    @Override
    public int hashCode() {
        return topicPartitions != null ? topicPartitions.hashCode() : 0;
    }

    /**
     * The topic partitions assigned to a group member.
     */
    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    @Override
    public String toString() {
        return "(topicPartitions=" + topicPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(",")) + ")";
    }
}
