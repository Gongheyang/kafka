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
package org.apache.kafka.server.log;

import org.apache.kafka.storage.internals.log.SkimpyOffsetMap;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OffsetMapTest {

    @Test
    public void testBasicValidation() throws NoSuchAlgorithmException {
        validateMap(10);
        validateMap(100);
        validateMap(1000);
        validateMap(5000);
    }

    @Test
    public void testClear() throws NoSuchAlgorithmException {
        SkimpyOffsetMap map = new SkimpyOffsetMap(4000);
        IntStream.range(0, 10).forEach(i -> {
            try {
                map.put(key(i), i);
            } catch (DigestException e) {
                throw new RuntimeException(e);
            }
        });
        IntStream.range(0, 10).forEach(i -> {
            try {
                assertEquals(i, map.get(key(i)));
            } catch (DigestException e) {
                throw new RuntimeException(e);
            }
        });
        map.clear();
        IntStream.range(0, 10).forEach(i -> {
            try {
                assertEquals(-1, map.get(key(i)));
            } catch (DigestException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testGetWhenFull() throws Exception {
        SkimpyOffsetMap map = new SkimpyOffsetMap(4096);
        int i = 37;
        while (map.size() < map.slots()) {
            map.put(key(i), i);
            i = i + 1;
        }
        assertEquals(map.get(key(i)), -1);
        assertEquals(map.get(key(i - 1)), i - 1);
    }

    private ByteBuffer key(Integer key) {
        return ByteBuffer.wrap(key.toString().getBytes());
    }

    private void validateMap(int items) throws NoSuchAlgorithmException {
        SkimpyOffsetMap map = new SkimpyOffsetMap(items * 48);
        IntStream.range(0, items).forEach(i -> {
            try {
                map.put(key(i), i);
            } catch (DigestException e) {
                throw new RuntimeException(e);
            }
        });
        IntStream.range(0, items).forEach(i -> {
            try {
                assertEquals(map.get(key(i)), i);
            } catch (DigestException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
