package org.apache.kafka.streams;

import java.time.Duration;
import java.util.Optional;

/**
 * Sets the {@code auto.offset.reset} configuration when
 * {@link #addSource(AutoOffsetReset, String, String...) adding a source processor} or when creating {@link KStream}
 * or {@link KTable} via {@link StreamsBuilder}.
 */
public class AutoOffsetReset {
    protected final Optional<Long> duration;

    protected AutoOffsetReset(Optional<Long> duration) {
        this.duration = duration;
    }

    /**
     * Creates an AutoOffsetReset instance representing "latest".
     * 
     * @return an AutoOffsetReset instance for the "latest" offset.
     */
    public static AutoOffsetReset latest() {
        return new AutoOffsetReset(Optional.empty());
    }

    /**
     * Creates an AutoOffsetReset instance representing "earliest".
     * 
     * @return an AutoOffsetReset instance for the "earliest" offset.
     */
    public static AutoOffsetReset earliest() {
        return new AutoOffsetReset(Optional.of(0L));
    }

    /**
     * Creates an AutoOffsetReset instance with a custom duration.
     * 
     * @param duration the duration to use for the offset reset; must be non-negative.
     * @return an AutoOffsetReset instance with the specified duration.
     * @throws IllegalArgumentException if the duration is negative.
     */
    public static AutoOffsetReset byDuration(final Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Duration cannot be negative");
        }
        return new AutoOffsetReset(Optional.of(duration.toMillis()));
    }

    /**
     * Retrieves the offset reset duration if specified.
     * 
     * @return an Optional containing the duration in milliseconds, or empty if "latest".
     */
    public Optional<Long> getDuration() {
        return duration;
    }

    @Override
    public String toString() {
        return duration.map(d -> "Duration: " + d + "ms").orElse("Latest");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoOffsetReset that = (AutoOffsetReset) o;
        return duration.equals(that.duration);
    }

    @Override
    public int hashCode() {
        return duration.hashCode();
    }
}
