/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RocksDBKeyValueStoreSupplierTest {

    @Test
    public void shouldRegisterWithLoggingEnabledWhenStoreLogged() throws Exception {
        final KeyValueStore<String, String> store = createStore(true, false);
        store.init(new MyMockProcessorContext() {
            @Override
            public void register(final StateStore store, final boolean loggingEnabled, final StateRestoreCallback func) {
                assertTrue("store should be registering as loggingEnabled", loggingEnabled);
            }
        }, store);
    }

    @Test
    public void shouldRegisterWithLoggingDisabledWhenStoreNotLogged() throws Exception {
        final KeyValueStore<String, String> store = createStore(false, false);
        store.init(new MyMockProcessorContext() {
            @Override
            public void register(final StateStore store, final boolean loggingEnabled, final StateRestoreCallback func) {
                assertFalse("store should not be registering as loggingEnabled", loggingEnabled);
            }

        }, store);
    }

    @Test
    public void shouldReturnCachedKeyValueStoreWhenCachingEnabled() throws Exception {
        assertThat(createStore(false, true), is(instanceOf(CachingKeyValueStore.class)));
    }

    @Test
    public void shouldReturnMeteredStoreWhenCachingAndLoggingDisabled() throws Exception {
        assertThat(createStore(false, false), is(instanceOf(MeteredKeyValueStore.class)));
    }

    @Test
    public void shouldReturnMeteredStoreWhenCachingDisabled() throws Exception {
        assertThat(createStore(true, false), is(instanceOf(MeteredKeyValueStore.class)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldHaveMeteredStoreWhenCached() throws Exception {
        final KeyValueStore store = createStore(false, true);
        final MockProcessorContext context = new MyMockProcessorContext();
        store.init(context, store);
        final StreamsMetrics metrics = context.metrics();
        assertFalse(metrics.metrics().isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldHaveMeteredStoreWhenLogged() throws Exception {
        final KeyValueStore store = createStore(true, false);
        final MockProcessorContext context = new MyMockProcessorContext();
        store.init(context, store);
        final StreamsMetrics metrics = context.metrics();
        assertFalse(metrics.metrics().isEmpty());
    }

    @SuppressWarnings("unchecked")
    private KeyValueStore<String, String> createStore(final boolean logged, final boolean cached) {
        return new RocksDBKeyValueStoreSupplier<>("name",
                                                  Serdes.String(),
                                                  Serdes.String(),
                                                  logged,
                                                  Collections.EMPTY_MAP,
                                                  cached).get();
    }

    private static class MyMockProcessorContext extends MockProcessorContext {
        MyMockProcessorContext() {
            super(new StateSerdes<>("", Serdes.String(), Serdes.String()), new NoOpRecordCollector());
        }

        @Override
        public ThreadCache getCache() {
            return new ThreadCache("foo", 0, new MockStreamsMetrics(new Metrics()));
        }

        @Override
        public File stateDir() {
            return TestUtils.tempDirectory();
        }
    }

}