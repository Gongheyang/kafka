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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents the strategy for resetting offsets in share consumer groups when no previous offset is found
 * for a partition or when an offset is out of range.
 * <p>
 * Supports three strategies:
 * <ul>
 *   <li>{@code EARLIEST} - Reset the offset to the earliest available offset
 *   <li>{@code LATEST} - Reset the offset to the latest available offset
 *   <li>{@code BY_DURATION} - Reset the offset to a timestamp that is the specified duration before the current time
 * </ul>
 * <p>
 * The strategy can be configured using string values:
 * <ul>
 *   <li>"earliest" for {@code EARLIEST}
 *   <li>"latest" for {@code LATEST}
 *   <li>"by_duration:&lt;duration&gt;" for {@code BY_DURATION}, where duration is in ISO-8601 format (e.g., "PT1H" for 1 hour)
 * </ul>
 */
public class ShareGroupAutoOffsetResetStrategy {
    public enum StrategyType {
        LATEST, EARLIEST, BY_DURATION;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    public static final ShareGroupAutoOffsetResetStrategy EARLIEST = new ShareGroupAutoOffsetResetStrategy(StrategyType.EARLIEST);
    public static final ShareGroupAutoOffsetResetStrategy LATEST = new ShareGroupAutoOffsetResetStrategy(StrategyType.LATEST);

    private final StrategyType type;
    private final Optional<Duration> duration;

    private ShareGroupAutoOffsetResetStrategy(StrategyType type) {
        this.type = type;
        this.duration = Optional.empty();
    }

    private ShareGroupAutoOffsetResetStrategy(Duration duration) {
        this.type = StrategyType.BY_DURATION;
        this.duration = Optional.of(duration);
    }

    /**
     *  Returns the AutoOffsetResetStrategy from the given string.
     */
    public static ShareGroupAutoOffsetResetStrategy fromString(String offsetStrategy) {
        if (offsetStrategy == null) {
            throw new IllegalArgumentException("Auto offset reset strategy is null");
        }

        if (StrategyType.BY_DURATION.toString().equals(offsetStrategy)) {
            throw new IllegalArgumentException("<:duration> part is missing in by_duration auto offset reset strategy.");
        }

        if (Arrays.asList(Utils.enumOptions(StrategyType.class)).contains(offsetStrategy)) {
            StrategyType type = StrategyType.valueOf(offsetStrategy.toUpperCase(Locale.ROOT));
            switch (type) {
                case EARLIEST:
                    return EARLIEST;
                case LATEST:
                    return LATEST;
                default:
                    throw new IllegalArgumentException("Unknown auto offset reset strategy: " + offsetStrategy);
            }
        }

        if (offsetStrategy.startsWith(StrategyType.BY_DURATION + ":")) {
            String isoDuration = offsetStrategy.substring(StrategyType.BY_DURATION.toString().length() + 1);
            try {
                Duration duration = Duration.parse(isoDuration);
                if (duration.isNegative()) {
                    throw new IllegalArgumentException("Negative duration is not supported in by_duration offset reset strategy.");
                }
                return new ShareGroupAutoOffsetResetStrategy(duration);
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to parse duration string in by_duration offset reset strategy.", e);
            }
        }

        throw new IllegalArgumentException("Unknown auto offset reset strategy: " + offsetStrategy);
    }

    /**
     * Returns the offset reset strategy type.
     */
    public StrategyType type() {
        return type;
    }

    /**
     * Returns the name of the offset reset strategy.
     */
    public String name() {
        return type.toString();
    }

    /**
     * Return the timestamp to be used for the ListOffsetsRequest.
     * @return the timestamp for the OffsetResetStrategy,
     * if the strategy is EARLIEST or LATEST or duration is provided
     * else return Optional.empty()
     */
    public Optional<Long> timestamp() {
        return switch (type) {
            case EARLIEST -> Optional.of(ListOffsetsRequest.EARLIEST_TIMESTAMP);
            case LATEST -> Optional.of(ListOffsetsRequest.LATEST_TIMESTAMP);
            case BY_DURATION -> duration.isPresent() 
                ? Optional.of(Instant.now().minus(duration.get()).toEpochMilli())
                : Optional.empty();
            default -> Optional.empty();
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShareGroupAutoOffsetResetStrategy that = (ShareGroupAutoOffsetResetStrategy) o;
        return type == that.type && Objects.equals(duration, that.duration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, duration);
    }

    @Override
    public String toString() {
        return "ShareGroupAutoOffsetReset{" +
                "type=" + type +
                (duration.map(value -> ", duration=" + value).orElse("")) +
                '}';
    }

    public static class Validator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            String offsetStrategy = (String) value;
            try {
                fromString(offsetStrategy);
            } catch (Exception e) {
                throw new ConfigException(name, value, "Invalid value `" + offsetStrategy + "` for configuration " +
                        name + ". The value must be either 'earliest', 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'.");
            }
        }
    }
}

