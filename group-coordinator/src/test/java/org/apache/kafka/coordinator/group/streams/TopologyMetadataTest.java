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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class TopologyMetadataTest {

    private Map<String, TopicMetadata> topicMetadata;
    private ConfiguredTopology configuredTopology;
    private TopologyMetadata topologyMetadata;

    @BeforeEach
    void setUp() {
        topicMetadata = new HashMap<>();
        configuredTopology = mock(ConfiguredTopology.class);
        topologyMetadata = new TopologyMetadata(topicMetadata, configuredTopology);
    }

    @Test
    void testTopicMetadata() {
        assertEquals(topicMetadata, topologyMetadata.topicMetadata());
    }

    @Test
    void testTopology() {
        assertEquals(configuredTopology, topologyMetadata.topology());
    }

    @Test
    void testIsStateful() {
        ConfiguredSubtopology subtopology1 = mock(ConfiguredSubtopology.class);
        ConfiguredSubtopology subtopology2 = mock(ConfiguredSubtopology.class);
        when(configuredTopology.subtopologies()).thenReturn(Map.of("subtopology1", subtopology1, "subtopology2", subtopology2));
        when(subtopology1.stateChangelogTopics()).thenReturn(Collections.singletonMap("state_changelog_topic", null));
        when(subtopology2.stateChangelogTopics()).thenReturn(Collections.emptyMap());

        assertTrue(topologyMetadata.isStateful("subtopology1"));
        assertFalse(topologyMetadata.isStateful("subtopology2"));
    }

    @Test
    void testNumPartitions() {
        ConfiguredSubtopology subtopology = mock(ConfiguredSubtopology.class);
        when(configuredTopology.subtopologies()).thenReturn(Collections.singletonMap("subtopology1", subtopology));
        when(subtopology.sourceTopics()).thenReturn(Collections.singleton("source_topic"));
        when(subtopology.repartitionSourceTopics()).thenReturn(Collections.singletonMap("repartition_source_topic", null));

        TopicMetadata topicMeta1 = mock(TopicMetadata.class);
        TopicMetadata topicMeta2 = mock(TopicMetadata.class);
        topicMetadata.put("source_topic", topicMeta1);
        topicMetadata.put("repartition_source_topic", topicMeta2);
        when(topicMeta1.numPartitions()).thenReturn(3);
        when(topicMeta2.numPartitions()).thenReturn(4);

        assertEquals(4, topologyMetadata.numPartitions("subtopology1"));
    }
}