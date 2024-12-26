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

import org.apache.kafka.common.metrics.Sensor;

/**
 * Maintains a ratio of the duration of a periodic event
 * over a rolling fixed-size window, emitting the measured ratio to the specified sensor for each window.
 * For example, this can be used to compute the ratio of
 * time that a thread is busy or idle.
 */
public class TimeRatio2 {

    private final long windowSizeMs;
    private final Sensor sensor;
    private long lastWindowEndMs;
    private long msSpentInEventInCurrentWindow;
    private long msSpentOutsideEventInCurrentWindow;
    private long lastEventStartMs;
    private long lastEventEndMs;

    public TimeRatio2(long windowSizeMs, Sensor sensor) {
        if (windowSizeMs < 0.0) {
            throw new IllegalArgumentException("Invalid window size: value " + windowSizeMs + " is less than 0.");
        }
        this.windowSizeMs = windowSizeMs;
        this.sensor = sensor;
    }

    private void updateWindow(long nowMs, long msSpentInEvent, long msSpentOutsideEvent) {
        long msSinceLastSamplingIntervalEnd = nowMs - this.lastWindowEndMs;
        if (msSinceLastSamplingIntervalEnd < this.windowSizeMs) {
            // We can't fill a window yet, so just add the last event's millis to the counters
            this.msSpentInEventInCurrentWindow += msSpentInEvent;
            this.msSpentOutsideEventInCurrentWindow += msSpentOutsideEvent;
        } else {
            // We can fill at least one window.
            // Add millis from the last event to fill this window and flush the ratio to the sensor,
            // then start a new window with any remaining millis we couldn't fit in the flushed window.
            // As each event period starts with "inside event" time and ends with "outside event" time,
            // we prefer to insert "inside event" time into the window first.
            long missingMsToCompleteWindow = this.windowSizeMs - (this.msSpentInEventInCurrentWindow + this.msSpentOutsideEventInCurrentWindow);
            long inEventMsFittingInWindow = Math.min(missingMsToCompleteWindow, msSpentInEvent);
            long outsideEventMsFittingInWindow = missingMsToCompleteWindow - inEventMsFittingInWindow;

            double eventRatio = (this.msSpentInEventInCurrentWindow + inEventMsFittingInWindow) * 1.0 / this.windowSizeMs;
            long currentWindowEndMs = this.lastWindowEndMs + this.windowSizeMs;
            this.sensor.record(eventRatio, currentWindowEndMs);

            this.msSpentInEventInCurrentWindow = 0;
            this.msSpentOutsideEventInCurrentWindow = 0;
            this.lastWindowEndMs = currentWindowEndMs;
            updateWindow(nowMs, msSpentInEvent - inEventMsFittingInWindow, msSpentOutsideEvent - outsideEventMsFittingInWindow);
        }
    }

    public void recordEventStart(long nowMs) {
        if (this.lastWindowEndMs == 0) {
            this.lastWindowEndMs = nowMs;
        } else {
            long msSpentInEvent = this.lastEventEndMs - this.lastEventStartMs;
            long msSpentOutsideEvent = nowMs - this.lastEventEndMs;
            updateWindow(nowMs, msSpentInEvent, msSpentOutsideEvent);
        }
        this.lastEventStartMs = nowMs;
    }

    public void recordEventEnd(long nowMs) {
        this.lastEventEndMs = nowMs;
    }

}
