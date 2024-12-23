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

package org.apache.kafka.connect.runtime.isolation;

import org.junit.jupiter.api.Test;

import java.io.IOException;


public class MultiVersionTest {

    @Test
    public void TestVersionedPluginLoaded() throws IOException {
        VersionedPluginBuilder builder = new VersionedPluginBuilder();
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.CONNECTOR, "0.0.0");
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.CONVERTER, "0.2.0");
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER, "0.3.0");
        builder.build("version_test_3");
    }
}
