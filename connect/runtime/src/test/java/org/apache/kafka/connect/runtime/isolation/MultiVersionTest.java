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

import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class MultiVersionTest {

    public void assertPluginLoad(Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts)
            throws InvalidVersionSpecificationException, ClassNotFoundException {

        String pluginPath = artifacts.keySet().stream().map(Path::toString).collect(Collectors.joining(","));
        Plugins plugins = new Plugins(Collections.singletonMap(WorkerConfig.PLUGIN_PATH_CONFIG, pluginPath));

        for (Map.Entry<Path, List<VersionedPluginBuilder.BuildInfo>> entry : artifacts.entrySet()) {
            String pluginLocation = entry.getKey().toAbsolutePath().toString();

            for (VersionedPluginBuilder.BuildInfo buildInfo : entry.getValue()) {
                for (String className : buildInfo.plugin().classNames()) {
                    ClassLoader pluginLoader = plugins.pluginLoader(className, PluginUtils.connectorVersionRequirement(buildInfo.version()));
                    Assertions.assertInstanceOf(PluginClassLoader.class, pluginLoader);
                    Assertions.assertTrue(((PluginClassLoader) pluginLoader).location().contains(pluginLocation));
                    Object p = plugins.newPlugin(className, PluginUtils.connectorVersionRequirement(buildInfo.version()));
                    Assertions.assertInstanceOf(Versioned.class, p);
                    Assertions.assertEquals(buildInfo.version(), ((Versioned) p).version());
                }
            }
        }
    }

    @Test
    public void TestVersionedPluginLoaded() throws IOException, InvalidVersionSpecificationException, ClassNotFoundException {
        VersionedPluginBuilder builder = new VersionedPluginBuilder();
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.CONNECTOR, "0.1.0");
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.CONVERTER, "0.2.0");
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER, "0.3.0");
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION, "0.4.0");
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.PREDICATE, "0.5.0");
        Path artifact = builder.build("version_test");
        assertPluginLoad(Collections.singletonMap(artifact, builder.buildInfos()));
    }
}
