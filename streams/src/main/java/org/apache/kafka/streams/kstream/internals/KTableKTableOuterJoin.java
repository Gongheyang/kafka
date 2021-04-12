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
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;
import static org.apache.kafka.streams.processor.internals.RecordQueue.UNKNOWN;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class KTableKTableOuterJoin<K, R, V1, V2> extends KTableKTableAbstractJoin<K, R, V1, V2> {
    private static final Logger LOG = LoggerFactory.getLogger(KTableKTableOuterJoin.class);

    KTableKTableOuterJoin(final KTableImpl<K, ?, V1> table1,
                          final KTableImpl<K, ?, V2> table2,
                          final ValueJoiner<? super V1, ? super V2, ? extends R> joiner) {
        super(table1, table2, joiner);
    }

    @Override
    public Processor<K, Change<V1>, K, Change<R>> get() {
        return new KTableKTableOuterJoinProcessor(valueGetterSupplier2.get());
    }

    @Override
    public KTableValueAndTimestampGetterSupplier<K, R> view() {
        return new KTableKTableOuterJoinValueGetterSupplier(valueGetterSupplier1, valueGetterSupplier2);
    }

    private class KTableKTableOuterJoinValueGetterSupplier extends KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2> {

        KTableKTableOuterJoinValueGetterSupplier(final KTableValueAndTimestampGetterSupplier<K, V1> valueGetterSupplier1,
                                                 final KTableValueAndTimestampGetterSupplier<K, V2> valueGetterSupplier2) {
            super(valueGetterSupplier1, valueGetterSupplier2);
        }

        public KTableValueAndTimestampGetter<K, R> get() {
            return new KTableKTableOuterJoinValueGetter(valueGetterSupplier1.get(), valueGetterSupplier2.get());
        }
    }

    private class KTableKTableOuterJoinProcessor extends ContextualProcessor<K, Change<V1>, K, Change<R>> {

        private final KTableValueAndTimestampGetter<K, V2> valueGetter;
        private StreamsMetricsImpl metrics;
        private Sensor droppedRecordsSensor;

        KTableKTableOuterJoinProcessor(final KTableValueAndTimestampGetter<K, V2> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @Override
        public void init(final ProcessorContext<K, Change<R>> context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
            valueGetter.init(context);
        }

        @Override
        public void process(final Record<K, Change<V1>> record) {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            if (record.key() == null) {
//                LOG.warn(
//                    "Skipping record due to null key. change=[{}] topic=[{}] partition=[{}] offset=[{}]",
//                    change, context().topic(), context().partition(), context().offset()
//                );
                droppedRecordsSensor.record();
                return;
            }

            R newValue = null;
            final long resultTimestamp;
            R oldValue = null;

            final ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter.get(record.key());
            final V2 value2 = getValueOrNull(valueAndTimestamp2);
            if (value2 == null) {
                if (record.value().newValue == null && record.value().oldValue == null) {
                    return;
                }
                resultTimestamp = record.timestamp();
            } else {
                resultTimestamp = Math.max(record.timestamp(), valueAndTimestamp2.timestamp());
            }

            if (value2 != null || record.value().newValue != null) {
                newValue = joiner.apply(record.value().newValue, value2);
            }

            if (sendOldValues && (value2 != null || record.value().oldValue != null)) {
                oldValue = joiner.apply(record.value().oldValue, value2);
            }

            context().forward(record.withValue(new Change<>(newValue, oldValue))
                .withTimestamp(resultTimestamp));
        }

        @Override
        public void close() {
            valueGetter.close();
        }
    }

    private class KTableKTableOuterJoinValueGetter implements KTableValueAndTimestampGetter<K, R> {

        private final KTableValueAndTimestampGetter<K, V1> valueGetter1;
        private final KTableValueAndTimestampGetter<K, V2> valueGetter2;

        KTableKTableOuterJoinValueGetter(final KTableValueAndTimestampGetter<K, V1> valueGetter1,
                                         final KTableValueAndTimestampGetter<K, V2> valueGetter2) {
            this.valueGetter1 = valueGetter1;
            this.valueGetter2 = valueGetter2;
        }

        @Override
        public <KParent, VParent> void init(ProcessorContext<KParent, VParent> context) {
            valueGetter1.init(context);
            valueGetter2.init(context);
        }

        @Override
        public ValueAndTimestamp<R> get(final K key) {
            R newValue = null;

            final ValueAndTimestamp<V1> valueAndTimestamp1 = valueGetter1.get(key);
            final V1 value1;
            final long timestamp1;
            if (valueAndTimestamp1 == null) {
                value1 = null;
                timestamp1 = UNKNOWN;
            } else {
                value1 = valueAndTimestamp1.value();
                timestamp1 = valueAndTimestamp1.timestamp();
            }

            final ValueAndTimestamp<V2> valueAndTimestamp2 = valueGetter2.get(key);
            final V2 value2;
            final long timestamp2;
            if (valueAndTimestamp2 == null) {
                value2 = null;
                timestamp2 = UNKNOWN;
            } else {
                value2 = valueAndTimestamp2.value();
                timestamp2 = valueAndTimestamp2.timestamp();
            }

            if (value1 != null || value2 != null) {
                newValue = joiner.apply(value1, value2);
            }

            return ValueAndTimestamp.make(newValue, Math.max(timestamp1, timestamp2));
        }

        @Override
        public void close() {
            valueGetter1.close();
            valueGetter2.close();
        }
    }

}
