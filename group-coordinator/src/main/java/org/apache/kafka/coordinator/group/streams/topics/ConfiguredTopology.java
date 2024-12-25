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
package org.apache.kafka.coordinator.group.streams.topics;

import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class captures the result of taking a topology definition sent by the client and using the current state of the topics inside the
 * broker to configure the internal topics required for the topology.
 */
public class ConfiguredTopology {

    /**
     * The epoch of the topology. Same as the topology epoch in the heartbeat request that last initialized the topology.
     */
    private final int topologyEpoch;

    /**
     * Contains the subtopologies that have been configured. This can be used by the task assignors, since it specifies the number of tasks
     * available for every subtopology.
     */
    private final Map<String, ConfiguredSubtopology> subtopologies;

    /**
     * Contains a list of internal topics that need to be created. This is used to create the topics in the broker.
     */
    private final Map<String, CreatableTopic> internalTopicsToBeCreated;

    /**
     * If the topic configuration process failed, e.g. because expected topics are missing or have an incorrect number of partitions, this
     * field will store the error that occurred, so that is can be reported back to the client.
     */
    private final Optional<TopicConfigurationException> topicConfigurationException;

    public ConfiguredTopology(final int topologyEpoch,
                              final Map<String, ConfiguredSubtopology> subtopologies,
                              final Map<String, CreatableTopic> internalTopicsToBeCreated,
                              final Optional<TopicConfigurationException> topicConfigurationException
    ) {
        this.topologyEpoch = topologyEpoch;
        this.subtopologies = subtopologies;
        this.internalTopicsToBeCreated = internalTopicsToBeCreated;
        this.topicConfigurationException = topicConfigurationException;
    }

    public int topologyEpoch() {
        return topologyEpoch;
    }

    public Map<String, ConfiguredSubtopology> subtopologies() {
        return subtopologies;
    }

    public Map<String, CreatableTopic> internalTopicsToBeCreated() {
        return internalTopicsToBeCreated;
    }

    public Optional<TopicConfigurationException> topicConfigurationException() {
        return topicConfigurationException;
    }

    public boolean isReady() {
        return topicConfigurationException.isEmpty();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ConfiguredTopology that = (ConfiguredTopology) o;
        return Objects.equals(topologyEpoch, that.topologyEpoch)
            && Objects.equals(subtopologies, that.subtopologies)
            && Objects.equals(internalTopicsToBeCreated, that.internalTopicsToBeCreated)
            && Objects.equals(topicConfigurationException, that.topicConfigurationException);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            topologyEpoch,
            subtopologies,
            internalTopicsToBeCreated,
            topicConfigurationException
        );
    }

    @Override
    public String toString() {
        return "ConfiguredTopology{" +
            "topologyEpoch='" + topologyEpoch + '\'' +
            ", subtopologies=" + subtopologies +
            ", internalTopicsToBeCreated=" + internalTopicsToBeCreated +
            ", topicConfigurationException=" + topicConfigurationException +
            '}';
    }

    public StreamsGroupDescribeResponseData.Topology asStreamsGroupDescribeTopology() {
        return new StreamsGroupDescribeResponseData.Topology()
            .setEpoch(topologyEpoch)
            .setSubtopologies(subtopologies.entrySet().stream().map(
                entry -> entry.getValue().asStreamsGroupDescribeSubtopology(entry.getKey())
            ).collect(Collectors.toList()));
    }

}
