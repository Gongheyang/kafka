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

package org.apache.kafka.controller;

import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Track the p99 for controller event queue processing time. If we encounter an event that takes longer
 * than this cached p99 time, we will log it at INFO level on the controller logger.
 */
public class EventPerformanceMonitor {
    /**
     * Don't report any p99 events below this threshold. This prevents the controller from reporting p99 event
     * times in the idle case where p99 event times are essentially the average as well.
     */
    private final long minSlowEventTimeNs;

    /**
     * Function that returns the current p99 time in millis. This call can be expensive, and since the histogram is
     * biased towards the last 5 minutes of data, we only need to update this p99 every so often.
     */
    private final Supplier<Double> thresholdMsSupplier;

    private final Logger log;

    /**
     * The current p99 threshold in nanos.
     */
    private long thresholdNs;

    private String slowestEvent;
    private long slowestDurationNs;


    public EventPerformanceMonitor(
        int minSlowEventTimeMs,
        Supplier<Double> thresholdMsSupplier,
        LogContext logContext
    ) {
        this.minSlowEventTimeNs = MILLISECONDS.toNanos(minSlowEventTimeMs);
        this.thresholdMsSupplier = thresholdMsSupplier;
        this.thresholdNs = minSlowEventTimeMs;
        this.log = logContext.logger(EventPerformanceMonitor.class);
        this.slowestEvent = null;
        this.slowestDurationNs = 0;
    }

    /**
     * Produce an INFO log if the given event ran for at least as long as the current p99 event processing time.
     *
     * @return true if a slow event was logged, false otherwise.
     */
    public boolean observeEvent(String name, long durationNs) {
        if (durationNs < minSlowEventTimeNs) {
            return false;
        }

        if (slowestEvent == null || slowestDurationNs < durationNs) {
            slowestEvent = name;
            slowestDurationNs = durationNs;
        }

        if (durationNs >= thresholdNs) {
            log.info("Slow controller event {} processed in {} us which is larger or equal to the p99 of {} us",
                name,
                NANOSECONDS.toMicros(durationNs),
                NANOSECONDS.toMicros(thresholdNs)
            );
            return true;
        }
        return false;
    }

    public void refreshPercentile() {
        if (slowestEvent != null) {
            log.info("Slowest event since last refresh was {} which processed in {} us",
                slowestEvent, NANOSECONDS.toMicros(slowestDurationNs));
            slowestEvent = null;
            slowestDurationNs = 0;
        }
        thresholdNs = (long) (thresholdMsSupplier.get() * 1000000);
        log.trace("Update slow controller event threshold (p99) to {} us.", NANOSECONDS.toMicros(thresholdNs));
    }
}
