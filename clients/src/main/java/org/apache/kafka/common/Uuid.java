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
package org.apache.kafka.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Set;

/**
 * This class defines an immutable universally unique identifier (UUID). It represents a 128-bit value.
 * More specifically, the random UUIDs generated by this class are variant 2 (Leach-Salz) version 4 UUIDs.
 * This is the same type of UUID as the ones generated by java.util.UUID. The toString() method prints
 * using the base64 string encoding. Likewise, the fromString method expects a base64 string encoding.
 */
public class Uuid implements Comparable<Uuid> {

    /**
     * A reserved UUID. Will never be returned by the randomUuid method.
     */
    public static final Uuid ONE_UUID = new Uuid(0L, 1L);

    /**
     * A UUID for the metadata topic in KRaft mode. Will never be returned by the randomUuid method.
     */
    public static final Uuid METADATA_TOPIC_ID = ONE_UUID;

    /**
     * A UUID that represents a null or empty UUID. Will never be returned by the randomUuid method.
     */
    public static final Uuid ZERO_UUID = new Uuid(0L, 0L);

    /**
     * The set of reserved UUIDs that will never be returned by the randomUuid method.
     */
    public static final Set<Uuid> RESERVED = Set.of(METADATA_TOPIC_ID, ZERO_UUID, ONE_UUID);

    private final long mostSignificantBits;
    private final long leastSignificantBits;

    /**
     * Constructs a 128-bit type 4 UUID where the first long represents the most significant 64 bits
     * and the second long represents the least significant 64 bits.
     */
    public Uuid(long mostSigBits, long leastSigBits) {
        this.mostSignificantBits = mostSigBits;
        this.leastSignificantBits = leastSigBits;
    }

    private static Uuid unsafeRandomUuid() {
        java.util.UUID jUuid = java.util.UUID.randomUUID();
        return new Uuid(jUuid.getMostSignificantBits(), jUuid.getLeastSignificantBits());
    }

    /**
     * Static factory to retrieve a type 4 (pseudo randomly generated) UUID.
     *
     * This will not generate a UUID equal to 0, 1, or one whose string representation starts with a dash ("-")
     */
    public static Uuid randomUuid() {
        Uuid uuid = unsafeRandomUuid();
        while (RESERVED.contains(uuid) || uuid.toString().startsWith("-")) {
            uuid = unsafeRandomUuid();
        }
        return uuid;
    }

    /**
     * Returns the most significant bits of the UUID's 128 value.
     */
    public long getMostSignificantBits() {
        return this.mostSignificantBits;
    }

    /**
     * Returns the least significant bits of the UUID's 128 value.
     */
    public long getLeastSignificantBits() {
        return this.leastSignificantBits;
    }

    /**
     * Returns true iff obj is another Uuid represented by the same two long values.
     */
    @Override
    public boolean equals(Object obj) {
        if ((null == obj) || (obj.getClass() != this.getClass()))
            return false;
        Uuid id = (Uuid) obj;
        return this.mostSignificantBits == id.mostSignificantBits &&
                this.leastSignificantBits == id.leastSignificantBits;
    }

    /**
     * Returns a hash code for this UUID
     */
    @Override
    public int hashCode() {
        long xor = mostSignificantBits ^ leastSignificantBits;
        return (int) (xor >> 32) ^ (int) xor;
    }

    /**
     * Returns a base64 string encoding of the UUID.
     */
    @Override
    public String toString() {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(getBytesFromUuid());
    }

    /**
     * Creates a UUID based on a base64 string encoding used in the toString() method.
     */
    public static Uuid fromString(String str) {
        if (str.length() > 24) {
            throw new IllegalArgumentException("Input string with prefix `"
                + str.substring(0, 24) + "` is too long to be decoded as a base64 UUID");
        }

        ByteBuffer uuidBytes = ByteBuffer.wrap(Base64.getUrlDecoder().decode(str));
        if (uuidBytes.remaining() != 16) {
            throw new IllegalArgumentException("Input string `" + str + "` decoded as "
                + uuidBytes.remaining() + " bytes, which is not equal to the expected 16 bytes "
                + "of a base64-encoded UUID");
        }

        return new Uuid(uuidBytes.getLong(), uuidBytes.getLong());
    }

    private byte[] getBytesFromUuid() {
        // Extract bytes for uuid which is 128 bits (or 16 bytes) long.
        ByteBuffer uuidBytes = ByteBuffer.wrap(new byte[16]);
        uuidBytes.putLong(this.mostSignificantBits);
        uuidBytes.putLong(this.leastSignificantBits);
        return uuidBytes.array();
    }

    @Override
    public int compareTo(Uuid other) {
        if (mostSignificantBits > other.mostSignificantBits) {
            return 1;
        } else if (mostSignificantBits < other.mostSignificantBits) {
            return -1;
        } else if (leastSignificantBits > other.leastSignificantBits) {
            return 1;
        } else if (leastSignificantBits < other.leastSignificantBits) {
            return -1;
        } else {
            return 0;
        }
    }

    /**
     * Convert a list of Uuid to an array of Uuid.
     *
     * @param list          The input list
     * @return              The output array
     */
    public static Uuid[] toArray(List<Uuid> list) {
        if (list == null) return null;
        Uuid[] array = new Uuid[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    /**
     * Convert an array of Uuids to a list of Uuid.
     *
     * @param array         The input array
     * @return              The output list
     */
    public static List<Uuid> toList(Uuid[] array) {
        if (array == null) return null;
        List<Uuid> list = new ArrayList<>(array.length);
        list.addAll(Arrays.asList(array));
        return list;
    }
}
