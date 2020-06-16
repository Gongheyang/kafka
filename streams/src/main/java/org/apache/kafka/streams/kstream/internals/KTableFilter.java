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

import static org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics.skippedIdempotentUpdatesSensor;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

class KTableFilter<K, V> implements KTableProcessorSupplier<K, V, V> {
    private final KTableImpl<K, ?, V> parent;
    private final Predicate<? super K, ? super V> predicate;
    private final boolean filterNot;
    private final String queryableName;
    private boolean sendOldValues = false;

    KTableFilter(final KTableImpl<K, ?, V> parent,
                 final Predicate<? super K, ? super V> predicate,
                 final boolean filterNot,
                 final String queryableName) {
        this.parent = parent;
        this.predicate = predicate;
        this.filterNot = filterNot;
        this.queryableName = queryableName;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableFilterProcessor();
    }

    @Override
    public void enableSendingOldValues() {
        parent.enableSendingOldValues();
        sendOldValues = true;
    }

    private V computeValue(final K key, final V value) {
        V newValue = null;

        if (value != null && (filterNot ^ predicate.test(key, value))) {
            newValue = value;
        }

        return newValue;
    }

    private ValueAndTimestamp<V> computeValue(final K key, final ValueAndTimestamp<V> valueAndTimestamp) {
        ValueAndTimestamp<V> newValueAndTimestamp = null;

        if (valueAndTimestamp != null) {
            final V value = valueAndTimestamp.value();
            if (filterNot ^ predicate.test(key, value)) {
                newValueAndTimestamp = valueAndTimestamp;
            }
        }

        return newValueAndTimestamp;
    }


    private class KTableFilterProcessor extends AbstractProcessor<K, Change<V>> {
        private MeteredTimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;
        private Sensor skippedIdempotentUpdatesSensor = null;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            if (queryableName != null) {
                final StateStore stateStore = context.getStateStore(queryableName);
                store = ((WrappedStateStore<MeteredTimestampedKeyValueStore<K, V>, K, V>) stateStore).wrapped();
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
                skippedIdempotentUpdatesSensor = skippedIdempotentUpdatesSensor(
                    Thread.currentThread().getName(), 
                    context.taskId().toString(), 
                    ((InternalProcessorContext) context).currentNode().name(), 
                    (StreamsMetricsImpl) context.metrics()
                );
            }
        }

        @Override
        public void process(final K key, final Change<V> change) {
            final V newValue = computeValue(key, change.newValue);
            final V oldValue = sendOldValues ? computeValue(key, change.oldValue) : null;

            if (sendOldValues && oldValue == null && newValue == null) {
                return; // unnecessary to forward here.
            }

            if (queryableName != null) {
                final byte[] serializedOldValue;
                if (oldValue != null) {
                    // 0 is a dummy timestamp
                    // we know that the old value must have a lesser timestamp than the new value
                    // therefore, we assigned it this value to ensure that if the two values are equal
                    // the new value is dropped
                    final ValueAndTimestamp<V> oldValueAndTimestamp = ValueAndTimestamp.make(oldValue, 0);
                    serializedOldValue = store.getSerializedValue(oldValueAndTimestamp);
                } else {
                    serializedOldValue = store.getWithBinary(key).serializedValue;
                }

                final ValueAndTimestamp<V> newValueAndTimestamp = ValueAndTimestamp.make(newValue,
                                                                                         context().timestamp());

                final boolean isDifferentValue = 
                        store.putIfDifferentValues(key, newValueAndTimestamp, serializedOldValue);
                if (isDifferentValue) {
                    tupleForwarder.maybeForward(key, newValue, oldValue);
                }  else {
                    skippedIdempotentUpdatesSensor.record();
                }
            } else {
                context().forward(key, new Change<>(newValue, oldValue));
            }
        }
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        // if the KTable is materialized, use the materialized store to return getter value;
        // otherwise rely on the parent getter and apply filter on-the-fly
        if (queryableName != null) {
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        } else {
            return new KTableValueGetterSupplier<K, V>() {
                final KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

                public KTableValueGetter<K, V> get() {
                    return new KTableFilterValueGetter(parentValueGetterSupplier.get());
                }

                @Override
                public String[] storeNames() {
                    return parentValueGetterSupplier.storeNames();
                }
            };
        }
    }


    private class KTableFilterValueGetter implements KTableValueGetter<K, V> {
        private final KTableValueGetter<K, V> parentGetter;

        KTableFilterValueGetter(final KTableValueGetter<K, V> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            parentGetter.init(context);
        }

        @Override
        public ValueAndTimestamp<V> get(final K key) {
            return computeValue(key, parentGetter.get(key));
        }

        @Override
        public void close() {
            parentGetter.close();
        }
    }

}
