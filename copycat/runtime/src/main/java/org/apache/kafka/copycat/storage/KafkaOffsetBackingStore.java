/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.storage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.ConvertingFutureCallback;
import org.apache.kafka.copycat.util.KafkaBasedLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 *     Implementation of OffsetBackingStore that uses a Kafka topic to store offset data.
 * </p>
 * <p>
 *     Internally, this implementation both produces to and consumes from a Kafka topic which stores the offsets.
 *     It accepts producer and consumer overrides via its configuration but forces some settings to specific values
 *     to ensure correct behavior (e.g. acks, auto.offset.reset).
 * </p>
 */
public class KafkaOffsetBackingStore implements OffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetBackingStore.class);

    public final static String OFFSET_STORAGE_TOPIC_CONFIG = "offset.storage.topic";

    private KafkaBasedLog<byte[], byte[]> offsetLog;
    private HashMap<ByteBuffer, ByteBuffer> data;

    @Override
    public void configure(Map<String, ?> configs) {
        String topic = (String) configs.get(OFFSET_STORAGE_TOPIC_CONFIG);
        if (topic == null)
            throw new CopycatException("Offset storage topic must be specified");

        data = new HashMap<>();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.putAll(configs);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.putAll(configs);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        offsetLog = createKafkaBasedLog(topic, producerProps, consumerProps, consumedCallback);
    }

    @Override
    public void start() {
        log.info("Starting KafkaOffsetBackingStore");
        offsetLog.start();
        log.info("Finished reading offsets topic and starting KafkaOffsetBackingStore");
    }

    @Override
    public void stop() {
        log.info("Stopping KafkaOffsetBackingStore");
        offsetLog.stop();
        log.info("Stopped KafkaOffsetBackingStore");
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys,
                                                   final Callback<Map<ByteBuffer, ByteBuffer>> callback) {
        ConvertingFutureCallback<Void, Map<ByteBuffer, ByteBuffer>> future = new ConvertingFutureCallback<Void, Map<ByteBuffer, ByteBuffer>>(callback) {
            @Override
            public Map<ByteBuffer, ByteBuffer> convert(Void result) {
                Map<ByteBuffer, ByteBuffer> values = new HashMap<>();
                for (ByteBuffer key : keys)
                    values.put(key, data.get(key));
                return values;
            }
        };
        // This operation may be relatively (but not too) expensive since it always requires checking end offsets, even
        // if we've already read up to the end. However, it also should not be common (offsets should only be read when
        // resetting a task). Always requiring that we read to the end is simpler than trying to differentiate when it
        // is safe not to (which should only be if we *know* we've maintained ownership since the last write).
        offsetLog.readToEnd(future);
        return future;
    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values, final Callback<Void> callback) {
        SetCallbackFuture producerCallback = new SetCallbackFuture(values.size(), callback);

        for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet())
            offsetLog.send(entry.getKey().array(), entry.getValue().array(), producerCallback);

        return producerCallback;
    }

    private final Callback<ConsumerRecord<byte[], byte[]>> consumedCallback = new Callback<ConsumerRecord<byte[], byte[]>>() {
        @Override
        public void onCompletion(Throwable error, ConsumerRecord<byte[], byte[]> record) {
            ByteBuffer key = record.key() != null ? ByteBuffer.wrap(record.key()) : null;
            ByteBuffer value = record.value() != null ? ByteBuffer.wrap(record.value()) : null;
            data.put(key, value);
        }
    };

    private KafkaBasedLog<byte[], byte[]> createKafkaBasedLog(String topic, Map<String, Object> producerProps,
                                                              Map<String, Object> consumerProps, Callback<ConsumerRecord<byte[], byte[]>> consumedCallback) {
        return new KafkaBasedLog<>(topic, producerProps, consumerProps, consumedCallback, new SystemTime());
    }

    private static class SetCallbackFuture implements org.apache.kafka.clients.producer.Callback, Future<Void> {
        private int numLeft;
        private boolean completed = false;
        private Throwable exception = null;
        private final Callback<Void> callback;

        public SetCallbackFuture(int numRecords, Callback<Void> callback) {
            numLeft = numRecords;
            this.callback = callback;
        }

        @Override
        public synchronized void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                if (!completed) {
                    this.exception = exception;
                    callback.onCompletion(exception, null);
                    completed = true;
                    this.notify();
                }
                return;
            }

            numLeft -= 1;
            if (numLeft == 0) {
                callback.onCompletion(null, null);
                completed = true;
                this.notify();
            }
        }

        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public synchronized boolean isCancelled() {
            return false;
        }

        @Override
        public synchronized boolean isDone() {
            return completed;
        }

        @Override
        public synchronized Void get() throws InterruptedException, ExecutionException {
            while (!completed) {
                this.wait();
            }
            if (exception != null)
                throw new ExecutionException(exception);
            return null;
        }

        @Override
        public synchronized Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            long started = System.currentTimeMillis();
            long limit = started + unit.toMillis(timeout);
            while (!completed) {
                long leftMs = limit - System.currentTimeMillis();
                if (leftMs < 0)
                    throw new TimeoutException("KafkaOffsetBackingStore Future timed out.");
                this.wait(leftMs);
            }
            if (exception != null)
                throw new ExecutionException(exception);
            return null;
        }
    }


}
