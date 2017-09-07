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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.protocol.types.FieldDef;

public class CommonFields {
    public static final FieldDef.Int32 THROTTLE_TIME_MS = new FieldDef.Int32("throttle_time_ms",
            "Duration in milliseconds for which the request was throttled due to quota violation (Zero if the " +
                    "request did not violate any quota)", 0);
    public static final FieldDef.Str TOPIC_NAME = new FieldDef.Str("topic", "Name of topic");
    public static final FieldDef.Int32 PARTITION_ID = new FieldDef.Int32("partition", "Topic partition id");
    public static final FieldDef.Int16 ERROR_CODE = new FieldDef.Int16("error_code", "Response error code");
    public static final FieldDef.NullableStr ERROR_MESSAGE = new FieldDef.NullableStr("error_message", "Response error message");

    // ACL Apis
    public static final FieldDef.Int8 RESOURCE_TYPE = new FieldDef.Int8("resource_type", "The resource type");
    public static final FieldDef.Str RESOURCE_NAME = new FieldDef.Str("resource_name", "The resource name");
    public static final FieldDef.NullableStr RESOURCE_NAME_FILTER = new FieldDef.NullableStr("resource_name", "The resource name filter");
    public static final FieldDef.Str PRINCIPAL = new FieldDef.Str("principal", "The ACL principal");
    public static final FieldDef.NullableStr PRINCIPAL_FILTER = new FieldDef.NullableStr("principal", "The ACL principal filter");
    public static final FieldDef.Str HOST = new FieldDef.Str("host", "The ACL host");
    public static final FieldDef.NullableStr HOST_FILTER = new FieldDef.NullableStr("host", "The ACL host filter");
    public static final FieldDef.Int8 OPERATION = new FieldDef.Int8("operation", "The ACL operation");
    public static final FieldDef.Int8 PERMISSION_TYPE = new FieldDef.Int8("permission_type", "The ACL permission type");
}
