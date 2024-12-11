package org.apache.kafka.controller.metrics;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.QuorumController;
import org.slf4j.Logger;

import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Track the p99 for controller event queue processing time. If we encounter an event that takes longer
 * than this cached p99 time, we will log it at INFO level on the controller logger.
 */
public class SlowEventsLogger {
    /**
     * Don't report any p99 events below this threshold. This prevents the controller from reporting p99 event
     * times in the idle case.
     */
    private static final int MIN_SLOW_EVENT_TIME_MS = 100;

    private final Supplier<Double> p99Supplier;
    private final Logger log;
    private double p99;

    public SlowEventsLogger(
        Supplier<Double> p99Supplier,
        LogContext logContext
    ) {
        this.p99Supplier = p99Supplier;
        this.p99 = p99Supplier.get();
        this.log = logContext.logger(SlowEventsLogger.class);
    }

    public void maybeLogEvent(String name, long durationNs) {
        long durationMs = MILLISECONDS.convert(durationNs, NANOSECONDS);
        if (durationMs > MIN_SLOW_EVENT_TIME_MS && durationMs > p99) {
            log.info("Slow controller event {} processed in {} us",
                name, MICROSECONDS.convert(durationNs, NANOSECONDS));
        }
    }

    public void refreshPercentile() {
        p99 = p99Supplier.get();
        log.trace("Update slow controller event threshold (p99) to {}", p99);
    }
}
