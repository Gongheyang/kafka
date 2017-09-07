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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT8;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class DescribeConfigsResponse extends AbstractResponse {

    private static final String RESOURCES_KEY_NAME = "resources";

    private static final String RESOURCE_TYPE_KEY_NAME = "resource_type";
    private static final String RESOURCE_NAME_KEY_NAME = "resource_name";

    private static final String CONFIG_ENTRIES_KEY_NAME = "config_entries";

    private static final String CONFIG_NAME_KEY_NAME = "config_name";
    private static final String CONFIG_VALUE_KEY_NAME = "config_value";
    private static final String IS_SENSITIVE_KEY_NAME = "is_sensitive";
    private static final String IS_DEFAULT_KEY_NAME = "is_default";
    private static final String READ_ONLY_KEY_NAME = "read_only";

    private static final Schema DESCRIBE_CONFIGS_RESPONSE_ENTITY_V0 = new Schema(
            new Field(ERROR_CODE),
            new Field(ERROR_MESSAGE),
            new Field(RESOURCE_TYPE_KEY_NAME, INT8),
            new Field(RESOURCE_NAME_KEY_NAME, STRING),
            new Field(CONFIG_ENTRIES_KEY_NAME, new ArrayOf(new Schema(
                    new Field(CONFIG_NAME_KEY_NAME, STRING),
                    new Field(CONFIG_VALUE_KEY_NAME, NULLABLE_STRING),
                    new Field(READ_ONLY_KEY_NAME, BOOLEAN),
                    new Field(IS_DEFAULT_KEY_NAME, BOOLEAN),
                    new Field(IS_SENSITIVE_KEY_NAME, BOOLEAN)))));

    private static final Schema DESCRIBE_CONFIGS_RESPONSE_V0 = new Schema(
            new Field(THROTTLE_TIME_MS),
            new Field(RESOURCES_KEY_NAME, new ArrayOf(DESCRIBE_CONFIGS_RESPONSE_ENTITY_V0)));

    public static Schema[] schemaVersions() {
        return new Schema[]{DESCRIBE_CONFIGS_RESPONSE_V0};
    }

    public static class Config {
        private final ApiError error;
        private final Collection<ConfigEntry> entries;

        public Config(ApiError error, Collection<ConfigEntry> entries) {
            this.error = error;
            this.entries = entries;
        }

        public ApiError error() {
            return error;
        }

        public Collection<ConfigEntry> entries() {
            return entries;
        }
    }

    public static class ConfigEntry {
        private final String name;
        private final String value;
        private final boolean isSensitive;
        private final boolean isDefault;
        private final boolean readOnly;

        public ConfigEntry(String name, String value, boolean isSensitive, boolean isDefault, boolean readOnly) {
            this.name = name;
            this.value = value;
            this.isSensitive = isSensitive;
            this.isDefault = isDefault;
            this.readOnly = readOnly;
        }

        public String name() {
            return name;
        }

        public String value() {
            return value;
        }

        public boolean isSensitive() {
            return isSensitive;
        }

        public boolean isDefault() {
            return isDefault;
        }

        public boolean isReadOnly() {
            return readOnly;
        }
    }

    private final int throttleTimeMs;
    private final Map<Resource, Config> configs;

    public DescribeConfigsResponse(int throttleTimeMs, Map<Resource, Config> configs) {
        this.throttleTimeMs = throttleTimeMs;
        this.configs = configs;
    }

    public DescribeConfigsResponse(Struct struct) {
        throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        Object[] resourcesArray = struct.getArray(RESOURCES_KEY_NAME);
        configs = new HashMap<>(resourcesArray.length);
        for (Object resourceObj : resourcesArray) {
            Struct resourceStruct = (Struct) resourceObj;

            ApiError error = new ApiError(resourceStruct);
            ResourceType resourceType = ResourceType.forId(resourceStruct.getByte(RESOURCE_TYPE_KEY_NAME));
            String resourceName = resourceStruct.getString(RESOURCE_NAME_KEY_NAME);
            Resource resource = new Resource(resourceType, resourceName);

            Object[] configEntriesArray = resourceStruct.getArray(CONFIG_ENTRIES_KEY_NAME);
            List<ConfigEntry> configEntries = new ArrayList<>(configEntriesArray.length);
            for (Object configEntriesObj: configEntriesArray) {
                Struct configEntriesStruct = (Struct) configEntriesObj;
                String configName = configEntriesStruct.getString(CONFIG_NAME_KEY_NAME);
                String configValue = configEntriesStruct.getString(CONFIG_VALUE_KEY_NAME);
                boolean isSensitive = configEntriesStruct.getBoolean(IS_SENSITIVE_KEY_NAME);
                boolean isDefault = configEntriesStruct.getBoolean(IS_DEFAULT_KEY_NAME);
                boolean readOnly = configEntriesStruct.getBoolean(READ_ONLY_KEY_NAME);
                configEntries.add(new ConfigEntry(configName, configValue, isSensitive, isDefault, readOnly));
            }
            Config config = new Config(error, configEntries);
            configs.put(resource, config);
        }
    }

    public Map<Resource, Config> configs() {
        return configs;
    }

    public Config config(Resource resource) {
        return configs.get(resource);
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DESCRIBE_CONFIGS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        List<Struct> resourceStructs = new ArrayList<>(configs.size());
        for (Map.Entry<Resource, Config> entry : configs.entrySet()) {
            Struct resourceStruct = struct.instance(RESOURCES_KEY_NAME);

            Resource resource = entry.getKey();
            resourceStruct.set(RESOURCE_TYPE_KEY_NAME, resource.type().id());
            resourceStruct.set(RESOURCE_NAME_KEY_NAME, resource.name());

            Config config = entry.getValue();
            config.error.write(resourceStruct);

            List<Struct> configEntryStructs = new ArrayList<>(config.entries.size());
            for (ConfigEntry configEntry : config.entries) {
                Struct configEntriesStruct = resourceStruct.instance(CONFIG_ENTRIES_KEY_NAME);
                configEntriesStruct.set(CONFIG_NAME_KEY_NAME, configEntry.name);
                configEntriesStruct.set(CONFIG_VALUE_KEY_NAME, configEntry.value);
                configEntriesStruct.set(IS_SENSITIVE_KEY_NAME, configEntry.isSensitive);
                configEntriesStruct.set(IS_DEFAULT_KEY_NAME, configEntry.isDefault);
                configEntriesStruct.set(READ_ONLY_KEY_NAME, configEntry.readOnly);
                configEntryStructs.add(configEntriesStruct);
            }
            resourceStruct.set(CONFIG_ENTRIES_KEY_NAME, configEntryStructs.toArray(new Struct[0]));
            
            resourceStructs.add(resourceStruct);
        }
        struct.set(RESOURCES_KEY_NAME, resourceStructs.toArray(new Struct[0]));
        return struct;
    }

    public static DescribeConfigsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeConfigsResponse(ApiKeys.DESCRIBE_CONFIGS.parseResponse(version, buffer));
    }

}
