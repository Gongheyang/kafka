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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfiguredTopologyTest {

    @Test
    public void testConstructorAndGetters() {
        int topologyEpoch = 1;
        Map<String, ConfiguredSubtopology> subtopologies = new HashMap<>();
        Map<String, CreatableTopic> internalTopicsToBeCreated = new HashMap<>();
        Optional<TopicConfigurationException> topicConfigurationException = Optional.empty();

        ConfiguredTopology configuredTopology = new ConfiguredTopology(
            topologyEpoch, subtopologies, internalTopicsToBeCreated, topicConfigurationException);

        assertEquals(topologyEpoch, configuredTopology.topologyEpoch());
        assertEquals(subtopologies, configuredTopology.subtopologies());
        assertEquals(internalTopicsToBeCreated, configuredTopology.internalTopicsToBeCreated());
        assertEquals(topicConfigurationException, configuredTopology.topicConfigurationException());
    }

    @Test
    public void testIsReady() {
        ConfiguredTopology readyTopology = new ConfiguredTopology(
            1, new HashMap<>(), new HashMap<>(), Optional.empty());
        assertTrue(readyTopology.isReady());

        ConfiguredTopology notReadyTopology = new ConfiguredTopology(
            1, new HashMap<>(), new HashMap<>(), Optional.of(TopicConfigurationException.missingSourceTopics("missing")));
        assertFalse(notReadyTopology.isReady());
    }

    @Test
    public void testEqualsAndHashCode() {
        int topologyEpoch = 1;
        Map<String, ConfiguredSubtopology> subtopologies = new HashMap<>();
        Map<String, CreatableTopic> internalTopicsToBeCreated = new HashMap<>();
        Optional<TopicConfigurationException> topicConfigurationException = Optional.empty();

        ConfiguredTopology configuredTopology1 = new ConfiguredTopology(
            topologyEpoch, subtopologies, internalTopicsToBeCreated, topicConfigurationException);
        ConfiguredTopology configuredTopology2 = new ConfiguredTopology(
            topologyEpoch, subtopologies, internalTopicsToBeCreated, topicConfigurationException);

        assertEquals(configuredTopology1, configuredTopology2);
        assertEquals(configuredTopology1.hashCode(), configuredTopology2.hashCode());
    }

    @Test
    public void testToString() {
        int topologyEpoch = 1;
        Map<String, ConfiguredSubtopology> subtopologies = new HashMap<>();
        Map<String, CreatableTopic> internalTopicsToBeCreated = new HashMap<>();
        Optional<TopicConfigurationException> topicConfigurationException = Optional.empty();

        ConfiguredTopology configuredTopology = new ConfiguredTopology(
            topologyEpoch, subtopologies, internalTopicsToBeCreated, topicConfigurationException);

        String expectedString = "ConfiguredTopology{" +
            "topologyEpoch='" + topologyEpoch + '\'' +
            ", subtopologies=" + subtopologies +
            ", internalTopicsToBeCreated=" + internalTopicsToBeCreated +
            ", topicConfigurationException=" + topicConfigurationException +
            '}';

        assertEquals(expectedString, configuredTopology.toString());
    }

    @Test
    public void testAsStreamsGroupDescribeTopology() {
        int topologyEpoch = 1;

        ConfiguredSubtopology subtopologyMock = mock(ConfiguredSubtopology.class);
        StreamsGroupDescribeResponseData.Subtopology subtopologyResponse = new StreamsGroupDescribeResponseData.Subtopology();
        when(subtopologyMock.asStreamsGroupDescribeSubtopology(Mockito.anyString())).thenReturn(subtopologyResponse);

        Map<String, ConfiguredSubtopology> subtopologies = new HashMap<>();
        subtopologies.put("subtopology1", subtopologyMock);

        Map<String, CreatableTopic> internalTopicsToBeCreated = new HashMap<>();
        Optional<TopicConfigurationException> topicConfigurationException = Optional.empty();

        ConfiguredTopology configuredTopology = new ConfiguredTopology(
            topologyEpoch, subtopologies, internalTopicsToBeCreated, topicConfigurationException);

        StreamsGroupDescribeResponseData.Topology topology = configuredTopology.asStreamsGroupDescribeTopology();

        assertEquals(topologyEpoch, topology.epoch());
        assertEquals(1, topology.subtopologies().size());
        assertEquals(subtopologyResponse, topology.subtopologies().get(0));
    }
}