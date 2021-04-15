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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class KTableKTableRightJoin<K, V, V1, VOut> extends KTableKTableAbstractJoin<K, V, V1, VOut> {
    private static final Logger LOG = LoggerFactory.getLogger(KTableKTableRightJoin.class);

    KTableKTableRightJoin(final KTableImpl<K, ?, V> table,
                          final KTableImpl<K, ?, V1> other,
                          final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner) {
        super(table, other, joiner);
    }

    @Override
    public Processor<K, Change<V>, K, Change<VOut>> get() {
        return new KTableKTableRightJoinProcessor(valueGetterSupplier2.get());
    }

    @Override
    public KTableValueGetterSupplier<K, VOut> view() {
        return new KTableKTableRightJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
    }

    private class KTableKTableRightJoinValueGetterSupplier extends KTableKTableAbstractJoinValueGetterSupplier<K, V, V1, VOut> {

        KTableKTableRightJoinValueGetterSupplier(final KTableValueGetterSupplier<K, V> valueGetterSupplier1,
                                                 final KTableValueGetterSupplier<K, V1> valueGetterSupplier2) {
            super(valueGetterSupplier1, valueGetterSupplier2);
        }

        public KTableValueGetter<K, VOut> get() {
            return new KTableKTableRightJoinValueGetter(valueGetterSupplier1.get(), valueGetterSupplier2.get());
        }
    }

    private class KTableKTableRightJoinProcessor extends ContextualProcessor<K, Change<V>, K, Change<VOut>> {

        private final KTableValueGetter<K, V1> valueGetter;
        private StreamsMetricsImpl metrics;
        private Sensor droppedRecordsSensor;

        KTableKTableRightJoinProcessor(final KTableValueGetter<K, V1> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @Override
        public void init(final ProcessorContext<K, Change<VOut>> context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
            valueGetter.init(context);
        }

        @Override
        public void process(final Record<K, Change<V>> record) {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            if (record.key() == null) {
                LOG.warn(
                    "Skipping record due to null key. change=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    record.value(),
                    context().recordMetadata().map(RecordMetadata::topic).orElse("<>"),
                    context().recordMetadata().map(RecordMetadata::partition).orElse(-1),
                    context().recordMetadata().map(RecordMetadata::offset).orElse(-1L)
                );
                droppedRecordsSensor.record();
                return;
            }

            final VOut newValue;
            final long resultTimestamp;
            VOut oldValue = null;

            final ValueAndTimestamp<V1> valueAndTimestampLeft = valueGetter.get(record.key());
            final V1 valueLeft = getValueOrNull(valueAndTimestampLeft);
            if (valueLeft == null) {
                return;
            }

            resultTimestamp = Math.max(record.timestamp(), valueAndTimestampLeft.timestamp());

            // joiner == "reverse joiner"
            newValue = joiner.apply(record.value().newValue, valueLeft);

            if (sendOldValues) {
                // joiner == "reverse joiner"
                oldValue = joiner.apply(record.value().oldValue, valueLeft);
            }

            context().forward(record.withValue(new Change<>(newValue, oldValue)).withTimestamp(resultTimestamp));
        }

        @Override
        public void close() {
            valueGetter.close();
        }
    }

    private class KTableKTableRightJoinValueGetter implements KTableValueGetter<K, VOut> {

        private final KTableValueGetter<K, V> valueGetter1;
        private final KTableValueGetter<K, V1> valueGetter2;

        KTableKTableRightJoinValueGetter(final KTableValueGetter<K, V> valueGetter1,
                                         final KTableValueGetter<K, V1> valueGetter2) {
            this.valueGetter1 = valueGetter1;
            this.valueGetter2 = valueGetter2;
        }

        @Override
        public <KParent, VParent> void init(final ProcessorContext<KParent, VParent> context) {
            valueGetter1.init(context);
            valueGetter2.init(context);
        }

        @Override
        public ValueAndTimestamp<VOut> get(final K key) {
            final ValueAndTimestamp<V1> valueAndTimestamp2 = valueGetter2.get(key);
            final V1 value2 = getValueOrNull(valueAndTimestamp2);

            if (value2 != null) {
                final ValueAndTimestamp<V> valueAndTimestamp1 = valueGetter1.get(key);
                final V value1 = getValueOrNull(valueAndTimestamp1);
                final long resultTimestamp;
                if (valueAndTimestamp1 == null) {
                    resultTimestamp = valueAndTimestamp2.timestamp();
                } else {
                    resultTimestamp = Math.max(valueAndTimestamp1.timestamp(), valueAndTimestamp2.timestamp());
                }
                return ValueAndTimestamp.make(joiner.apply(value1, value2), resultTimestamp);
            } else {
                return null;
            }
        }

        @Override
        public void close() {
            valueGetter1.close();
            valueGetter2.close();
        }
    }

}
