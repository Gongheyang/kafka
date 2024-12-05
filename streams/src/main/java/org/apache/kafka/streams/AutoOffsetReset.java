package org.apache.kafka.streams;

import java.time.Duration;
import java.util.Optional;

/**
 * Sets the {@code auto.offset.reset} configuration when
 * {@link #addSource(AutoOffsetReset, String, String...) adding a source processor} or when creating {@link KStream}
 * or {@link KTable} via {@link StreamsBuilder}.
 */
public class AutoOffsetReset {

    private enum OffsetResetType {
        LATEST,
        EARLIEST,
        BY_DURATION
    }

    private final OffsetResetType type;
    private final Optional<Long> duration;

    private AutoOffsetReset(OffsetResetType type, Optional<Long> duration) {
        this.type = type;
        this.duration = duration;
    }

    /**
     * Creates an AutoOffsetReset instance representing "latest".
     * 
     * @return an AutoOffsetReset instance for the "latest" offset.
     */
    public static AutoOffsetReset latest() {
        return new AutoOffsetReset(OffsetResetType.LATEST, Optional.empty());
    }

    /**
     * Creates an AutoOffsetReset instance representing "earliest".
     * 
     * @return an AutoOffsetReset instance for the "earliest" offset.
     */
    public static AutoOffsetReset earliest() {
        return new AutoOffsetReset(OffsetResetType.EARLIEST, Optional.empty());
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
        return new AutoOffsetReset(OffsetResetType.BY_DURATION, Optional.of(duration.toMillis()));
    }

    /**
     * Provides a human-readable description of the offset reset type and duration.
     * 
     * @return a string describing the offset reset configuration.
     */
    public String describe() {
        switch (type) {
            case LATEST:
                return "Offset: Latest";
            case EARLIEST:
                return "Offset: Earliest";
            case BY_DURATION:
                return "Offset by duration: " + duration.map(d -> d + "ms").orElse("Invalid duration");
            default:
                throw new IllegalStateException("Unexpected type: " + type);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AutoOffsetReset that = (AutoOffsetReset) o;
        return type == that.type && duration.equals(that.duration);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + duration.hashCode();
        return result;
    }
}
