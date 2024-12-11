package org.apache.kafka.controller.metrics;

import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

public class SlowEventsLoggerTest {
    @Test
    public void test() {
        LogContext logContext = new LogContext();
        AtomicReference<Double> p99 = new AtomicReference<>(1000.0);
        SlowEventsLogger logger = new SlowEventsLogger(p99::get, logContext);

        logger.maybeLogEvent();
    }
}
