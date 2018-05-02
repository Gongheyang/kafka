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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.ForwardingDisabledProcessorContext;

public class KStreamTransformValues<K, V, R> implements ProcessorSupplier<K, V> {

    private final InternalValueTransformerWithKeySupplier<K, V, R> valueTransformerSupplier;

    public KStreamTransformValues(final InternalValueTransformerWithKeySupplier<K, V, R> valueTransformerSupplier) {
        this.valueTransformerSupplier = valueTransformerSupplier;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamTransformValuesProcessor<>(valueTransformerSupplier.get());
    }

    public static class KStreamTransformValuesProcessor<K, V, R> implements Processor<K, V> {

        private final InternalValueTransformerWithKey<K, V, R> valueTransformer;
        private ProcessorContext context;

        public KStreamTransformValuesProcessor(final InternalValueTransformerWithKey<K, V, R> valueTransformer) {
            this.valueTransformer = valueTransformer;
        }

        @Override
        public void init(final ProcessorContext context) {
            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
            this.context = context;
        }

        @Override
        public void process(K key, V value) {
            context.forward(key, valueTransformer.transform(key, value));
        }

        @SuppressWarnings("deprecation")
        @Override
        public void punctuate(long timestamp) {
            if (valueTransformer.punctuate(timestamp) != null) {
                throw new StreamsException("ValueTransformer#punctuate must return null.");
            }
        }

        @Override
        public void close() {
            valueTransformer.close();
        }
    }
}
