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
package org.apache.kafka.streams;
     
/**
  * Sets the {@code auto.offset.reset} configuration when
  * {@link #addSource(AutoOffsetReset, String, String...) adding a source processor} or when creating {@link KStream}
  * or {@link KTable} via {@link StreamsBuilder}.
  */
 
import java.time.Duration; 
import java.util.Optional;
public class AutoOffsetReset {
 
    public static AutoOffsetReset LATEST =  new AutoOffsetReset("latest");
    public static AutoOffsetReset EARLIEST =  new AutoOffsetReset("earliest");
 
    protected final String name;
    protected final Optional<Long> duration;
 
    private AutoOffsetReset(String name, long duration) {
        this.name = name;
        this.duration = Optional.of(duration);
    }
 
    private AutoOffsetReset(String name) {
        this.name = name;
        this.duration = Optional.empty();
    }
 
    public static AutoOffsetReset latest() {
        return LATEST;
    }
 
    public static AutoOffsetReset earliest() {
        return EARLIEST;
    }
 
    public static AutoOffsetReset back(Duration duration) {
        return new AutoOffsetReset("back", duration.toMillis());
    }
}