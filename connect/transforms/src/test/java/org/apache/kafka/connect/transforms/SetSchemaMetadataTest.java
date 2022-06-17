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
package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SetSchemaMetadataTest {
    private final SetSchemaMetadata<SinkRecord> xform = new SetSchemaMetadata.Value<>();

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaNameUpdate() {
        xform.configure(Collections.singletonMap("schema.name", "foo"));
        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);
        final SinkRecord updatedRecord = xform.apply(record);
        assertEquals("foo", updatedRecord.valueSchema().name());
    }

    @Test
    public void schemaNameUpdateWithSchemaNamespaceAppend() {
        final Map<String, String> props = new HashMap<>();
        props.put("schema.namespace", "foo.bar");
        props.put("schema.name", "baz");
        xform.configure(props);
        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);
        final SinkRecord updatedRecord = xform.apply(record);
        assertEquals("foo.bar.baz", updatedRecord.valueSchema().name());
    }

    @Test
    public void schemaNamespaceAppend() {
        xform.configure(Collections.singletonMap("schema.namespace", "foo.bar"));
        Schema schema = SchemaBuilder.struct().name("baz").build();
        final SinkRecord record = new SinkRecord("", 0, null, null, schema, null, 0);
        final SinkRecord updatedRecord = xform.apply(record);
        assertEquals("foo.bar.baz", updatedRecord.valueSchema().name());
    }

    @Test
    public void schemaNamespaceAppendWithMissingSchemaName() {
        xform.configure(Collections.singletonMap("schema.namespace", "foo.bar"));
        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);
        assertThrows(DataException.class, () -> xform.apply(record));
    }

    @Test
    public void schemaVersionUpdate() {
        xform.configure(Collections.singletonMap("schema.version", 42));
        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);
        final SinkRecord updatedRecord = xform.apply(record);
        assertEquals(Integer.valueOf(42), updatedRecord.valueSchema().version());
    }

    @Test
    public void schemaNameAndVersionUpdate() {
        final Map<String, String> props = new HashMap<>();
        props.put("schema.name", "foo");
        props.put("schema.version", "42");

        xform.configure(props);

        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);

        final SinkRecord updatedRecord = xform.apply(record);

        assertEquals("foo", updatedRecord.valueSchema().name());
        assertEquals(Integer.valueOf(42), updatedRecord.valueSchema().version());
    }

    @Test
    public void schemaNameWithNamespaceAndVersionUpdate() {
        final Map<String, String> props = new HashMap<>();
        props.put("schema.namespace", "foo.bar");
        props.put("schema.name", "baz");
        props.put("schema.version", "42");

        xform.configure(props);

        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);

        final SinkRecord updatedRecord = xform.apply(record);

        assertEquals("foo.bar.baz", updatedRecord.valueSchema().name());
        assertEquals(Integer.valueOf(42), updatedRecord.valueSchema().version());
    }

    @Test
    public void schemaNameWithNamespaceAndVersionUpdateWithStruct() {
        final String fieldName1 = "f1";
        final String fieldName2 = "f2";
        final String fieldValue1 = "value1";
        final int fieldValue2 = 1;
        final Schema schema = SchemaBuilder.struct()
                                      .name("my.orig.SchemaDefn")
                                      .field(fieldName1, Schema.STRING_SCHEMA)
                                      .field(fieldName2, Schema.INT32_SCHEMA)
                                      .build();
        final Struct value = new Struct(schema).put(fieldName1, fieldValue1).put(fieldName2, fieldValue2);

        final Map<String, String> props = new HashMap<>();
        props.put("schema.namespace", "foo.bar");
        props.put("schema.name", "baz");
        props.put("schema.version", "42");
        xform.configure(props);

        final SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

        final SinkRecord updatedRecord = xform.apply(record);
        Struct newValue = (Struct) updatedRecord.value();

        assertEquals("foo.bar.baz", updatedRecord.valueSchema().name());
        assertEquals(Integer.valueOf(42), updatedRecord.valueSchema().version());

        // Make sure the struct's schema and fields all point to the new schema
        assertMatchingSchema(newValue.schema(), updatedRecord.valueSchema());
    }

    @Test
    public void valueSchemaRequired() {
        final SinkRecord record = new SinkRecord("", 0, null, null, null, 42, 0);
        assertThrows(DataException.class, () -> xform.apply(record));
    }

    @Test
    public void ignoreRecordWithNullValue() {
        final SinkRecord record = new SinkRecord("", 0, null, null, null, null, 0);

        final SinkRecord updatedRecord = xform.apply(record);

        assertNull(updatedRecord.key());
        assertNull(updatedRecord.keySchema());
        assertNull(updatedRecord.value());
        assertNull(updatedRecord.valueSchema());
    }

    @Test
    public void updateSchemaOfStruct() {
        final String fieldName1 = "f1";
        final String fieldName2 = "f2";
        final String fieldValue1 = "value1";
        final int fieldValue2 = 1;
        final Schema schema = SchemaBuilder.struct()
                                      .name("my.orig.SchemaDefn")
                                      .field(fieldName1, Schema.STRING_SCHEMA)
                                      .field(fieldName2, Schema.INT32_SCHEMA)
                                      .build();
        final Struct value = new Struct(schema).put(fieldName1, fieldValue1).put(fieldName2, fieldValue2);

        final Schema newSchema = SchemaBuilder.struct()
                                      .name("my.updated.SchemaDefn")
                                      .field(fieldName1, Schema.STRING_SCHEMA)
                                      .field(fieldName2, Schema.INT32_SCHEMA)
                                      .build();

        Struct newValue = (Struct) SetSchemaMetadata.updateSchemaIn(value, newSchema);
        assertMatchingSchema(newValue.schema(), newSchema);
    }

    @Test
    public void updateSchemaOfNonStruct() {
        Object value = 1;
        Object updatedValue = SetSchemaMetadata.updateSchemaIn(value, Schema.INT32_SCHEMA);
        assertSame(value, updatedValue);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void updateSchemaOfNull() {
        Object updatedValue = SetSchemaMetadata.updateSchemaIn(null, Schema.INT32_SCHEMA);
        assertNull(updatedValue);
    }

    protected void assertMatchingSchema(Schema actual, Schema expected) {
        assertSame(expected, actual);
        assertEquals(expected.name(), actual.name());
        for (Field field : expected.fields()) {
            String fieldName = field.name();
            assertEquals(expected.field(fieldName).name(), actual.field(fieldName).name());
            assertEquals(expected.field(fieldName).index(), actual.field(fieldName).index());
            assertSame(expected.field(fieldName).schema(), actual.field(fieldName).schema());
        }
    }
}
