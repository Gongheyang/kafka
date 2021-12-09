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
package org.apache.kafka.streams.query;

import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.common.utils.Bytes;

@Evolving
public class RawKeyQuery implements Query<byte[]> {

    private final Bytes key;

    private RawKeyQuery(final Bytes key) {
        this.key = key;
    }

    public static RawKeyQuery withKey(final Bytes key) {
        return new RawKeyQuery(key);
    }

    public static RawKeyQuery withKey(final byte[] key) {
        return new RawKeyQuery(Bytes.wrap(key));
    }

    public Bytes getKey() {
        return key;
    }
}
