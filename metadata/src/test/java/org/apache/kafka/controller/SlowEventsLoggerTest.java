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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class SlowEventsLoggerTest {
    @Test
    public void testSlowEvents() {
        LogContext logContext = new LogContext();

        AtomicReference<Double> p99 = new AtomicReference<>(0.0);
        SlowEventsLogger logger = new SlowEventsLogger(100, p99::get, logContext);

        // Initially, the p99 is zero
        assertFalse(logger.maybeLogEvent("test", MILLISECONDS.toNanos(10)));
        assertFalse(logger.maybeLogEvent("test", MILLISECONDS.toNanos(99)));
        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(100)));


        // Idle controller, low p99
        p99.set(30.0);
        logger.refreshPercentile();
        assertFalse(logger.maybeLogEvent("test", MILLISECONDS.toNanos(90)));
        assertFalse(logger.maybeLogEvent("test", MILLISECONDS.toNanos(99)));
        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(100)));

        // Busy controller, high p99
        p99.set(1000.0);
        logger.refreshPercentile();
        assertFalse(logger.maybeLogEvent("test", MILLISECONDS.toNanos(100)));
        assertFalse(logger.maybeLogEvent("test", MILLISECONDS.toNanos(200)));
        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(1000)));
        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(2000)));
    }

    @Test
    public void testThresholdDisabled() {
        LogContext logContext = new LogContext();

        AtomicReference<Double> p99 = new AtomicReference<>(0.0);
        // Set min slow event time to zero, effectively disabling the threshold
        SlowEventsLogger logger = new SlowEventsLogger(0, p99::get, logContext);

        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(0)));
        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(10)));
        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(99)));
        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(100)));

        p99.set(30.0);
        logger.refreshPercentile();
        assertFalse(logger.maybeLogEvent("test", MILLISECONDS.toNanos(0)));
        assertFalse(logger.maybeLogEvent("test", MILLISECONDS.toNanos(29)));
        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(30)));
        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(100)));


        p99.set(1000.0);
        logger.refreshPercentile();
        assertFalse(logger.maybeLogEvent("test", MILLISECONDS.toNanos(100)));
        assertFalse(logger.maybeLogEvent("test", MILLISECONDS.toNanos(999)));
        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(1000)));
        assertTrue(logger.maybeLogEvent("test", MILLISECONDS.toNanos(2000)));
    }
}
