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
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Interface for storing the aggregated values of sessions.
 * <p>
 * The key is internally represented as {@link Windowed Windowed&lt;K&gt;} that comprises the plain key
 * and the {@link Window} that represents window start- and end-timestamp.
 * In contrast to a {@link SessionStore} that stores windowedKeys-value pairs,
 * a {@code TimestampedSessionStore} stores windowedKeys-(value/timestamp) pairs.
 * <p>
 * If two sessions are merged, a new session with new start- and end-timestamp must be inserted into the store
 * while the two old sessions must be deleted.
 * If an existing session is updated (without changing the session boundaries), the value-side timestamp is used
 * to store the last update timestamp of the corresponding session.
 *
 * @param <K>   type of the record keys
 * @param <AGG> type of the aggregated values
 */
public interface TimestampedSessionStore<K, AGG> extends SessionStore<K, ValueAndTimestamp<AGG>> { }