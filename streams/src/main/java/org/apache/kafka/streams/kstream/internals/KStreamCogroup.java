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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;

public class KStreamCogroup<K, V> implements KStreamAggProcessorSupplier<K, K, Change<V>, V> {

    private final List<KStreamAggregate<K, ?, V>> parents;

    private boolean sendOldValues = false;

    public KStreamCogroup(Collection<KStreamAggregate<K, ?, V>> parents) {
        this.parents = new ArrayList<>(parents);
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KStreamCogroupProcessor<>();
    }
    
    private static final class KStreamCogroupProcessor<K, V> extends AbstractProcessor<K, Change<V>> {
        @Override
        public void process(K key, Change<V> value) {
            context().forward(key, value);
        }
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        return parents.get(0).view();
    }

    @Override
    public void enableSendingOldValues() {
        if (!sendOldValues) {
            for (KStreamAggProcessorSupplier<?, ?, ?, ?> parent : parents) {
                parent.enableSendingOldValues();
            }
        }
        sendOldValues = true;
    }
}
