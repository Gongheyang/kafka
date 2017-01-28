/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FlattenTest {

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        final Flatten<SourceRecord> xform = new Flatten.Value<>();
        xform.configure(Collections.<String, String>emptyMap());
        xform.apply(new SourceRecord(null, null, "topic", 0, Schema.INT32_SCHEMA, 42));
    }

    @Test(expected = DataException.class)
    public void topLevelMapRequired() {
        final Flatten<SourceRecord> xform = new Flatten.Value<>();
        xform.configure(Collections.<String, String>emptyMap());
        xform.apply(new SourceRecord(null, null, "topic", 0, null, 42));
    }

    @Test
    public void testNestedStruct() {
        final Flatten<SourceRecord> xform = new Flatten.Value<>();
        xform.configure(Collections.<String, String>emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("int8", Schema.INT8_SCHEMA);
        builder.field("int16", Schema.INT16_SCHEMA);
        builder.field("int32", Schema.INT32_SCHEMA);
        builder.field("int64", Schema.INT64_SCHEMA);
        builder.field("float32", Schema.FLOAT32_SCHEMA);
        builder.field("float64", Schema.FLOAT64_SCHEMA);
        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
        builder.field("string", Schema.STRING_SCHEMA);
        builder.field("bytes", Schema.BYTES_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("B", supportedTypesSchema);
        Schema oneLevelNestedSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("A", oneLevelNestedSchema);
        Schema twoLevelNestedSchema = builder.build();

        Struct supportedTypes = new Struct(supportedTypesSchema);
        supportedTypes.put("int8", (byte) 8);
        supportedTypes.put("int16", (short) 16);
        supportedTypes.put("int32", 32);
        supportedTypes.put("int64", (long) 64);
        supportedTypes.put("float32", 32.f);
        supportedTypes.put("float64", 64.);
        supportedTypes.put("boolean", true);
        supportedTypes.put("string", "stringy");
        supportedTypes.put("bytes", "bytes".getBytes());

        Struct oneLevelNestedStruct = new Struct(oneLevelNestedSchema);
        oneLevelNestedStruct.put("B", supportedTypes);

        Struct twoLevelNestedStruct = new Struct(twoLevelNestedSchema);
        twoLevelNestedStruct.put("A", oneLevelNestedStruct);

        SourceRecord transformed = xform.apply(new SourceRecord(null, null,
                "topic", 0,
                twoLevelNestedSchema, twoLevelNestedStruct));

        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertEquals(9, transformedStruct.schema().fields().size());
        assertEquals(8, (byte) transformedStruct.getInt8("A.B.int8"));
        assertEquals(16, (short) transformedStruct.getInt16("A.B.int16"));
        assertEquals(32, (int) transformedStruct.getInt32("A.B.int32"));
        assertEquals(64L, (long) transformedStruct.getInt64("A.B.int64"));
        assertEquals(32.f, transformedStruct.getFloat32("A.B.float32"), 0.f);
        assertEquals(64., transformedStruct.getFloat64("A.B.float64"), 0.);
        assertEquals(true, transformedStruct.getBoolean("A.B.boolean"));
        assertEquals("stringy", transformedStruct.getString("A.B.string"));
        assertArrayEquals("bytes".getBytes(), transformedStruct.getBytes("A.B.bytes"));
    }

    @Test
    public void testNestedMapWithDelimiter() {
        final Flatten<SourceRecord> xform = new Flatten.Value<>();
        xform.configure(Collections.singletonMap("delimiter", "#"));

        Map<String, Object> supportedTypes = new HashMap<>();
        supportedTypes.put("int8", (byte) 8);
        supportedTypes.put("int16", (short) 16);
        supportedTypes.put("int32", 32);
        supportedTypes.put("int64", (long) 64);
        supportedTypes.put("float32", 32.f);
        supportedTypes.put("float64", 64.);
        supportedTypes.put("boolean", true);
        supportedTypes.put("string", "stringy");
        supportedTypes.put("bytes", "bytes".getBytes());

        Map<String, Object> oneLevelNestedMap = Collections.singletonMap("B", (Object) supportedTypes);
        Map<String, Object> twoLevelNestedMap = Collections.singletonMap("A", (Object) oneLevelNestedMap);

        SourceRecord transformed = xform.apply(new SourceRecord(null, null,
                "topic", 0,
                null, twoLevelNestedMap));

        assertNull(transformed.valueSchema());
        assertTrue(transformed.value() instanceof Map);
        Map<String, Object> transformedMap = (Map<String, Object>) transformed.value();
        assertEquals(9, transformedMap.size());
        assertEquals((byte) 8, transformedMap.get("A#B#int8"));
        assertEquals((short) 16, transformedMap.get("A#B#int16"));
        assertEquals(32, transformedMap.get("A#B#int32"));
        assertEquals((long) 64, transformedMap.get("A#B#int64"));
        assertEquals(32.f, (float) transformedMap.get("A#B#float32"), 0.f);
        assertEquals(64., (double) transformedMap.get("A#B#float64"), 0.);
        assertEquals(true, transformedMap.get("A#B#boolean"));
        assertEquals("stringy", transformedMap.get("A#B#string"));
        assertArrayEquals("bytes".getBytes(), (byte[]) transformedMap.get("A#B#bytes"));
    }

    @Test
    public void testOptionalFieldStruct() {
        final Flatten<SourceRecord> xform = new Flatten.Value<>();
        xform.configure(Collections.<String, String>emptyMap());

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA);
        Schema supportedTypesSchema = builder.build();

        builder = SchemaBuilder.struct();
        builder.field("B", supportedTypesSchema);
        Schema oneLevelNestedSchema = builder.build();

        Struct supportedTypes = new Struct(supportedTypesSchema);
        supportedTypes.put("opt_int32", null);

        Struct oneLevelNestedStruct = new Struct(oneLevelNestedSchema);
        oneLevelNestedStruct.put("B", supportedTypes);

        SourceRecord transformed = xform.apply(new SourceRecord(null, null,
                "topic", 0,
                oneLevelNestedSchema, oneLevelNestedStruct));

        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type());
        Struct transformedStruct = (Struct) transformed.value();
        assertNull(transformedStruct.get("B.opt_int32"));
    }

    @Test
    public void testOptionalFieldMap() {
        final Flatten<SourceRecord> xform = new Flatten.Value<>();
        xform.configure(Collections.<String, String>emptyMap());

        Map<String, Object> supportedTypes = new HashMap<>();
        supportedTypes.put("opt_int32", null);

        Map<String, Object> oneLevelNestedMap = Collections.singletonMap("B", (Object) supportedTypes);

        SourceRecord transformed = xform.apply(new SourceRecord(null, null,
                "topic", 0,
                null, oneLevelNestedMap));

        assertNull(transformed.valueSchema());
        assertTrue(transformed.value() instanceof Map);
        Map<String, Object> transformedMap = (Map<String, Object>) transformed.value();

        assertNull(transformedMap.get("B.opt_int32"));
    }
}
