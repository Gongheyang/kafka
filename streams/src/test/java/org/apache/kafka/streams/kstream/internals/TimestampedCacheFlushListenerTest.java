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

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.Test;

public class TimestampedCacheFlushListenerTest {

    @Test
    public void shouldForwardValueTimestampIfNewValueExists() {
        final InternalProcessorContext<String, Change<String>> context = mock(InternalProcessorContext.class);
        expect(context.currentNode()).andReturn(null).anyTimes();
        context.setCurrentNode(null);
        context.setCurrentNode(null);
        context.forward(
            new Record<>(
                "key",
                new Change<>("newValue", "oldValue"),
                42L));
        expectLastCall();
        replay(context);

        final TupleChangeCacheFlushListener<String, String> flushListener = new TupleChangeCacheFlushListener<>(
            context);
        flushListener.apply(
            "key",
            ValueAndTimestamp.make("newValue", 42L),
            ValueAndTimestamp.make("oldValue", 21L),
            73L);

        verify(context);
    }

    @Test
    public void shouldForwardParameterTimestampIfNewValueIsNull() {
        final InternalProcessorContext<String, Change<String>> context = mock(InternalProcessorContext.class);
        expect(context.currentNode()).andReturn(null).anyTimes();
        context.setCurrentNode(null);
        context.setCurrentNode(null);
        context.forward(
            new Record<>(
                "key",
                new Change<>(null, "oldValue"),
                73L));
        expectLastCall();
        replay(context);

        new TupleChangeCacheFlushListener<>(context).apply(
            "key",
            null,
            ValueAndTimestamp.make("oldValue", 21L),
            73L);

        verify(context);
    }
}
