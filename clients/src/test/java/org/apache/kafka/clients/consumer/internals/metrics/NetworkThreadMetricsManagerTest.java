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

package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.lang.Double.NaN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class NetworkThreadMetricsManagerTest {
    private final Time time = new MockTime();
    private final Metrics metrics = new Metrics(time);

    @Test
    public void testTimeBetweenNetworkThreadPollMetrics() {
        final long shortTimeToSleep = 100;
        NetworkThreadMetricsManager networkThreadMetricsManager = new NetworkThreadMetricsManager(metrics);

        // Assert the existence of metrics
        assertNotNull(metrics.metric(networkThreadMetricsManager.maxPollTime));
        assertNotNull(metrics.metric(networkThreadMetricsManager.avgPollTime));

        // Record poll time and sleep for short amount of time
        long currentTimeMs = time.milliseconds();
        networkThreadMetricsManager.updatePollTime(currentTimeMs);
        time.sleep(shortTimeToSleep);

        // Record poll time
        currentTimeMs = time.milliseconds();
        networkThreadMetricsManager.updatePollTime(currentTimeMs);

        // Randomly sleep 1-10 seconds
        Random rand = new Random();
        int randomSleepTime = rand.nextInt(10) + 1;
        time.sleep(TimeUnit.SECONDS.toMillis(randomSleepTime));

        // Record poll time
        currentTimeMs = time.milliseconds();
        networkThreadMetricsManager.updatePollTime(currentTimeMs);

        // Calculating expected average time between polls
        long totalTimeBetweenPolls = randomSleepTime * 1000 + shortTimeToSleep;

        assertEquals(metrics.metric(networkThreadMetricsManager.avgPollTime).metricValue(), totalTimeBetweenPolls / 2d);
        assertEquals(metrics.metric(networkThreadMetricsManager.maxPollTime).metricValue(), randomSleepTime * 1000d);
    }

    @Test
    public void testApplicationEventQueueSizeMetric() {
        NetworkThreadMetricsManager networkThreadMetricsManager = new NetworkThreadMetricsManager(metrics);

        // Assert the existence of metrics
        assertNotNull(metrics.metric(networkThreadMetricsManager.applicationEventQueueSize));

        // Record application event queue size
        networkThreadMetricsManager.recordApplicationEventQueueSize(10);
        networkThreadMetricsManager.recordApplicationEventQueueSize(20);
        networkThreadMetricsManager.recordApplicationEventQueueSize(30);

        // Assert recorded metrics values
        assertEquals(metrics.metric(networkThreadMetricsManager.applicationEventQueueSize).metricValue(), 30d);
    }

    @Test
    public void testApplicationEventQueueChangeMetrics() {
        NetworkThreadMetricsManager networkThreadMetricsManager = new NetworkThreadMetricsManager(metrics);
        PollEvent pollEvent = new PollEvent(0);

        // Assert the existence of metrics
        assertNotNull(metrics.metric(networkThreadMetricsManager.avgAppEventTime));
        assertNotNull(metrics.metric(networkThreadMetricsManager.maxAppEventTime));

        // Record application event queue change
        networkThreadMetricsManager.recordApplicationEventQueueChange(pollEvent, time.milliseconds(), true);

        // Assert recorded metrics values
        assertEquals(metrics.metric(networkThreadMetricsManager.avgAppEventTime).metricValue(), NaN);
        assertEquals(metrics.metric(networkThreadMetricsManager.maxAppEventTime).metricValue(), NaN);
    }
}
