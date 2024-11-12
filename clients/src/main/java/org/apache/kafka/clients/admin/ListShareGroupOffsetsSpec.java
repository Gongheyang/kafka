package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Specification of share group offsets to list using {@link Admin#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)}.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListShareGroupOffsetsSpec {

    private Collection<TopicPartition> topicPartitions;

    public ListShareGroupOffsetsSpec() {
        topicPartitions = new ArrayList<>();
    }

    /**
     * Set the topic partitions whose offsets are to be listed for a share group.
     */
    ListShareGroupOffsetsSpec topicPartitions(Collection<TopicPartition> topicPartitions) {
        this.topicPartitions = topicPartitions;
        return this;
    }

    /**
     * Returns the topic partitions whose offsets are to be listed for a share group.
     */
    Collection<TopicPartition> topicPartitions() {
        return topicPartitions;
    }
}
