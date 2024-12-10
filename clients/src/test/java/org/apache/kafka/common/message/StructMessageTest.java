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
package org.apache.kafka.common.message;

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StructMessageTest {

    @Test
    public void testDefaultValues() {
        StructMessageData message = new StructMessageData();
        assertNull(message.nullableStruct);
        assertEquals(new StructMessageData.MyStruct2(), message.nullableStruct2);
        assertNull(message.nullableStruct3);
        assertEquals(new StructMessageData.MyStruct4(), message.nullableStruct4);

        message = roundTrip(message, (short) 2);
        assertNull(message.nullableStruct);
        assertEquals(new StructMessageData.MyStruct2(), message.nullableStruct2);
        assertNull(message.nullableStruct3);
        assertEquals(new StructMessageData.MyStruct4(), message.nullableStruct4);
    }

    @Test
    public void testRoundTrip() {
        StructMessageData message = new StructMessageData()
            .setNullableStruct(new StructMessageData.MyStruct()
                .setMyInt(1)
                .setMyString("1"))
            .setNullableStruct2(new StructMessageData.MyStruct2()
                .setMyInt(2)
                .setMyString("2"))
            .setNullableStruct3(new StructMessageData.MyStruct3()
                .setMyInt(3)
                .setMyString("3"))
            .setNullableStruct4(new StructMessageData.MyStruct4()
                .setMyInt(4)
                .setMyString("4"))
            .setNonNullableStruct(new StructMessageData.MyStruct5()
                .setMyInt(5)
                .setMyString("5"));

        StructMessageData newMessage = roundTrip(message, (short) 2);
        assertEquals(message, newMessage);
    }

    @Test
    public void testNullForNullableFields() {
        StructMessageData message = new StructMessageData()
            .setNullableStruct(null)
            .setNullableStruct2(null)
            .setNullableStruct3(null)
            .setNullableStruct4(null);

        message = roundTrip(message, (short) 2);
        assertNull(message.nullableStruct);
        assertNull(message.nullableStruct2);
        assertNull(message.nullableStruct3);
        assertNull(message.nullableStruct4);
    }

    @Test
    public void testNullableStruct2CanNotBeNullInVersion0() {
        StructMessageData message = new StructMessageData()
            .setNullableStruct2(null);

        assertThrows(NullPointerException.class, () -> roundTrip(message, (short) 0));
    }

    @Test
    public void testToStringWithNullStructs() {
        StructMessageData message = new StructMessageData()
            .setNullableStruct(null)
            .setNullableStruct2(null)
            .setNullableStruct3(null)
            .setNullableStruct4(null);

        message.toString();
    }

    @Test
    public void testTaggedStructSizing() {
        StructMessageData message = new StructMessageData()
            .setNullableStruct(null)
            .setNullableStruct2(null)
            .setNullableStruct3(null)
            .setNullableStruct4(new StructMessageData.MyStruct4()
                .setMyInt(4)
                .setMyString(new String(new char[121])))
            .setNonNullableStruct(new StructMessageData.MyStruct5()
                .setMyInt(5)
                .setMyString(new String(new char[121])));

        // We want the structs to be 127 bytes long, so that the varint encoding of their size is
        // one short of overflowing into a two-byte representation. An extra byte is added to the
        // nullable struct size to account for the is-null flag.
        assertEquals(127, message.nullableStruct4().size(new ObjectSerializationCache(), (short) 2));
        assertEquals(127, message.nonNullableStruct().size(new ObjectSerializationCache(), (short) 2));

        StructMessageData newMessage = roundTrip(message, (short) 2);
        assertEquals(message, newMessage);
    }

    private StructMessageData deserialize(ByteBuffer buf, short version) {
        StructMessageData message = new StructMessageData();
        message.read(new ByteBufferAccessor(buf.duplicate()), version);
        return message;
    }

    private ByteBuffer serialize(StructMessageData message, short version) {
        return MessageUtil.toByteBuffer(message, version);
    }

    private StructMessageData roundTrip(StructMessageData message, short version) {
        ByteBuffer buffer = serialize(message, version);
        assertEquals(buffer.remaining(), message.size(new ObjectSerializationCache(), version));
        return deserialize(buffer.duplicate(), version);
    }
}
