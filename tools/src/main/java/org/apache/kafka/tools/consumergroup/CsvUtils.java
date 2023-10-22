/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.tools.consumergroup;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

public class CsvUtils {
    private final CsvMapper mapper = new CsvMapper();

    ObjectReader readerFor(Class<? extends CsvRecord> clazz) {
        return mapper.readerFor(clazz).with(getSchema(clazz));
    }

    ObjectWriter writerFor(Class<? extends CsvRecord> clazz) {
        return mapper.writerFor(clazz).with(getSchema(clazz));
    }

    private CsvSchema getSchema(Class<? extends CsvRecord> clazz) {
        String[] fields;
        if (CsvRecordWithGroup.class == clazz)
            fields = CsvRecordWithGroup.FIELDS;
        else if (CsvRecordNoGroup.class == clazz)
            fields = CsvRecordNoGroup.FIELDS;
        else
            throw new IllegalStateException("Unhandled class " + clazz);

        return mapper.schemaFor(clazz).sortedBy(fields);
    }

    public interface CsvRecord {
    }

    public static class CsvRecordWithGroup implements CsvRecord {
        public static final String[] FIELDS = new String[] {"group", "topic", "partition", "offset"};

        public final String group;
        public final String topic;
        public final int partition;
        public final long offset;

        public CsvRecordWithGroup(String group, String topic, int partition, long offset) {
            this.group = group;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }
    }

    public static class CsvRecordNoGroup implements CsvRecord {
        public static final String[] FIELDS = new String[]{"topic", "partition", "offset"};

        public final String topic;
        public final int partition;
        public final long offset;

        public CsvRecordNoGroup(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }
    }
}
