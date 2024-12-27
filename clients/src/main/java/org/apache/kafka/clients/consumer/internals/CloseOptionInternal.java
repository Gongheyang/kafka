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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.CloseOption;
import org.apache.kafka.clients.consumer.GroupMembershipOperation;

import java.time.Duration;
import java.util.Optional;

/**
 * This class represents an internal version of {@link CloseOption}, used for internal processing of consumer shutdown options.
 *
 * <p>While {@link CloseOption} is the user-facing class, {@code CloseOptionInternal} is intended for accessing internal fields
 * and performing operations within the Kafka codebase. This class should not be used directly by users.
 * It extends {@link CloseOption} and provides getters for the {@link GroupMembershipOperation} and timeout values.</p>
 */
public class CloseOptionInternal extends CloseOption {
    CloseOptionInternal(final CloseOption option) {
        super(option);
    }

    public GroupMembershipOperation groupMembershipOperation() {
        return operation;
    }

    public Optional<Duration> timeout() {
        return timeout;
    }
}
