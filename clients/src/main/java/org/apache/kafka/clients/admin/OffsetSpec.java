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
package org.apache.kafka.clients.admin;

import java.util.Map;

/** 
 * This class allows to specify the desired offsets when using {@link KafkaAdminClient#listOffsets(Map, ListOffsetsOptions)}
 */
public class OffsetSpec {

    public static class EarliestSpec extends OffsetSpec { }
    public static class LatestSpec extends OffsetSpec { }
    public static class MaxTimestampSpec extends OffsetSpec { }
    public static class EarliestLocalSpec extends OffsetSpec { }
    public static class LatestTierSpec extends OffsetSpec { }
    public static class TimestampSpec extends OffsetSpec {
        private final long timestamp;

        TimestampSpec(long timestamp) {
            this.timestamp = timestamp;
        }

        long timestamp() {
            return timestamp;
        }
    }

    /**
     * Used to retrieve the latest offset of a partition
     */
    public static OffsetSpec latest() {
        return new LatestSpec();
    }

    /**
     * Used to retrieve the earliest offset of a partition
     */
    public static OffsetSpec earliest() {
        return new EarliestSpec();
    }

    /**
     * Used to retrieve the earliest offset whose timestamp is greater than
     * or equal to the given timestamp in the corresponding partition
     * @param timestamp in milliseconds
     */
    public static OffsetSpec forTimestamp(long timestamp) {
        return new TimestampSpec(timestamp);
    }

    /**
     * Used to retrieve the offset with the largest timestamp of a partition
     * as message timestamps can be specified client side this may not match
     * the log end offset returned by LatestSpec
     */
    public static OffsetSpec maxTimestamp() {
        return new MaxTimestampSpec();
    }

    /**
     * Used to retrieve the offset with the local log start offset,
     * log start offset is the offset of a log above which reads are guaranteed to be served
     * from the disk of the leader broker, when Tiered Storage is not enabled, it behaves the same
     * as the earliest timestamp
     */
    public static OffsetSpec earliestLocalSpec() {
        return new EarliestLocalSpec();
    }

    /**
     * Used to retrieve the offset with the highest offset of data stored in remote storage,
     * and when Tiered Storage is not enabled, we won't return any offset (i.e. Unknown offset)
     */
    public static OffsetSpec latestTierSpec() {
        return new LatestTierSpec();
    }
}
