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
package org.apache.kafka.clients.consumer.internals;

public enum TempFetchMode {

    SKIP_BUFFERED(1),   // Skip buffered partitions (this is the existing behavior)
    SKIP_FETCH(2),      // Skip the fetch request altogether if there are any buffered partitions
    INCLUDE_NOMINAL(3), // Include buffered partitions, but use a nominal max bytes value in the request
    SKIP_NODE(4);       // Skip each node that has any buffered partitions

    private final int option;

    TempFetchMode(int option) {
        this.option = option;
    }

    /**
     * Case-insensitive lookup by string name.
     */
    public static TempFetchMode of(final int option) {
        for (TempFetchMode mode : TempFetchMode.values()) {
            if (mode.option == option)
                return mode;
        }

        throw new IllegalArgumentException("No " + TempFetchMode.class.getSimpleName() + " for option value " + option);
    }

    public int option() {
        return option;
    }

    public String optionString() {
        return String.valueOf(option);
    }

    @Override
    public String toString() {
        return "TempFetchMode{" + "option=" + option + '}';
    }
}
