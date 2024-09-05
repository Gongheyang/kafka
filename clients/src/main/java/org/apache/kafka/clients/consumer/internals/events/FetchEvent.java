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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.FetchRequestManager;

/**
 * {@code FetchEvent} is sent from the consumer to signal that we want to issue a fetch request for the partitions
 * to which the consumer is currently subscribed.
 *
 * <p/>
 *
 * <em>Note</em>: this event is completed when the {@link FetchRequestManager} has finished performing the
 * fetch request process. It does not mean that the requests are complete. It could be the case that no fetch
 * requests were created. Also of note is that if any fetch requests were created.
 */
public class FetchEvent extends CompletableApplicationEvent<Void> {

    public FetchEvent(final long deadlineMs) {
        super(Type.FETCH, deadlineMs);
    }
}
