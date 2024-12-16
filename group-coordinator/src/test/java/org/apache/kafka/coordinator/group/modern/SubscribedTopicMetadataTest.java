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
package org.apache.kafka.coordinator.group.modern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.MetadataImageBuilder;
import org.apache.kafka.image.MetadataImage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SubscribedTopicMetadataTest {

    private Map<Uuid, TopicMetadata> topicMetadataMap;
    private SubscribedTopicDescriberImpl subscribedTopicMetadata;
    private MetadataImage metadataImage;
    private final int numPartitions = 5;

    @BeforeEach
    public void setUp() {
        MetadataImageBuilder metadataImageBuilder = new MetadataImageBuilder();
        IntStream.range(0, 5).forEach(i -> {
            Uuid topicId = Uuid.randomUuid();
            String topicName = "topic" + i;
            metadataImageBuilder.addTopic(topicId, topicName, numPartitions);
        });
        metadataImageBuilder.addRacks();
        metadataImage = metadataImageBuilder.build();

        topicMetadataMap = new HashMap<>();
        metadataImage.topics().topicsById().forEach((topicId, topicImage) -> {
            topicMetadataMap.put(
                topicId,
                new TopicMetadata(topicId, topicImage.name(), ModernGroup.computeTopicHash(topicImage, metadataImage.cluster()))
            );
        });
        subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadataMap, metadataImage);
    }

    @Test
    public void testAttribute() {
        assertEquals(topicMetadataMap, subscribedTopicMetadata.topicMetadata());
        assertEquals(metadataImage, subscribedTopicMetadata.metadataImage());
    }

    @Test
    public void testTopicMetadataCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new SubscribedTopicDescriberImpl(null, metadataImage));
    }

    @Test
    public void testMetadataImageCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new SubscribedTopicDescriberImpl(topicMetadataMap, null));
    }

    @Test
    public void testNumberOfPartitions() {
        Uuid topicId = Uuid.randomUuid();

        // Test -1 is returned when the topic Id doesn't exist.
        assertEquals(-1, subscribedTopicMetadata.numPartitions(topicId));

        // Test that the correct number of partitions are returned for a given topic Id.
        topicMetadataMap.forEach((id, metadata) -> {
            // Test that the correct number of partitions are returned for a given topic Id.
            assertEquals(numPartitions, subscribedTopicMetadata.numPartitions(id));
        });
    }

    @Test
    public void testEquals() {
        assertEquals(new SubscribedTopicDescriberImpl(topicMetadataMap, metadataImage), subscribedTopicMetadata);

        Map<Uuid, TopicMetadata> topicMetadataMap2 = new HashMap<>();
        Uuid topicId = Uuid.randomUuid();
        MetadataImage metadataImage2 = new MetadataImageBuilder()
            .addTopic(topicId, "newTopic", 5)
            .addRacks()
            .build();
        topicMetadataMap2.put(
            topicId,
            new TopicMetadata(topicId, "newTopic", ModernGroup.computeTopicHash(metadataImage2.topics().getTopic(topicId), metadataImage2.cluster()))
        );
        assertNotEquals(new SubscribedTopicDescriberImpl(topicMetadataMap2, metadataImage2), subscribedTopicMetadata);
    }
}
