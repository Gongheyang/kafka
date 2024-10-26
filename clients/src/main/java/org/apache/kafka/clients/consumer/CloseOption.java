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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.ConsumerUtils;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

public class CloseOption {

    /**
     * Specifies the group membership operation upon shutdown.
     * By default, {@code GroupMembershipOperation.DEFAULT} will be applied, which follows the consumer's default behavior.
     */
    protected GroupMembershipOperation operation = GroupMembershipOperation.DEFAULT;

    /**
     * Specifies the maximum amount of time to wait for the close process to complete.
     * This allows users to define a custom timeout for gracefully stopping the consumer.
     * If no value is set, the default timeout {@link ConsumerUtils#DEFAULT_CLOSE_TIMEOUT_MS} will be applied.
     */
    protected Optional<Duration> timeout = Optional.empty();

    private CloseOption() {
    }

    protected CloseOption(final CloseOption option) {
        this.operation = option.operation;
        this.timeout = option.timeout;
    }

    /**
     * Static method to create a {@code CloseOption} with a custom timeout.
     *
     * @param timeout the maximum time to wait for the consumer to close.
     * @return a new {@code CloseOption} instance with the specified timeout.
     */
    public static CloseOption timeout(final Duration timeout) {
        CloseOption option = new CloseOption();
        option.timeout = Optional.ofNullable(timeout);
        return option;
    }

    /**
     * Static method to create a {@code CloseOption} with a specified group membership operation.
     *
     * @param operation the group membership operation to apply. Must be one of {@code LEAVE_GROUP}, {@code REMAIN_IN_GROUP},
     *                  or {@code DEFAULT}.
     * @return a new {@code CloseOption} instance with the specified group membership operation.
     */
    public static CloseOption groupMembershipOperation(final GroupMembershipOperation operation) {
        Objects.requireNonNull(operation, "operation should not be null");
        CloseOption option = new CloseOption();
        option.operation = operation;
        return option;
    }

    /**
     * Fluent method to set the timeout for the close process.
     *
     * @param timeout the maximum time to wait for the consumer to close. If {@code null}, the default timeout will be used.
     * @return this {@code CloseOption} instance.
     */
    public CloseOption withTimeout(final Duration timeout) {
        this.timeout = Optional.ofNullable(timeout);
        return this;
    }

    /**
     * Fluent method to set the group membership operation upon shutdown.
     *
     * @param operation the group membership operation to apply. Must be one of {@code LEAVE_GROUP}, {@code REMAIN_IN_GROUP}, or {@code DEFAULT}.
     * @return this {@code CloseOption} instance.
     */
    public CloseOption withGroupMembershipOperation(final GroupMembershipOperation operation) {
        Objects.requireNonNull(operation, "operation should not be null");
        this.operation = operation;
        return this;
    }
}
