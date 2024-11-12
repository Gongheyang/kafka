package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * The result of the {@link Admin#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListShareGroupOffsetsResult {

    private final Map<String, KafkaFuture<Map<TopicPartition, Long>>> futures;

    public ListShareGroupOffsetsResult(Map<String, KafkaFuture<Map<TopicPartition, Long>>> futures) {
        this.futures = futures;
    }

    /**
     * Return a future which yields all Map<String, Map<TopicPartition, Long> objects, if requests for all the groups succeed.
     */
    public KafkaFuture<Map<String, Map<TopicPartition, Long>>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(
                nil -> {
                    Map<String, Map<TopicPartition, Long>> offsets = new HashMap<>(futures.size());
                    futures.forEach((key, future) -> {
                        try {
                            offsets.put(key, future.get());
                        } catch (InterruptedException | ExecutionException e) {
                            // This should be unreachable, since the KafkaFuture#allOf already ensured
                            // that all the futures completed successfully.
                            throw new RuntimeException(e);
                        }
                    });
                    return offsets;
                });
    }

    /**
     * Return a future which yields a map of topic partitions to offsets for the specified group.
     */
    public KafkaFuture<Map<TopicPartition, Long>> partitionsToOffset(String groupId) {
        KafkaFutureImpl<Map<TopicPartition, Long>> future = new KafkaFutureImpl<>();
        if (futures.containsKey(groupId))
            return futures.get(groupId);
        else
            future.completeExceptionally(new IllegalArgumentException("Group ID not found: " + groupId));
        return future;
    }
}