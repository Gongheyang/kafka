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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class VersionedPluginBuilder {

    private static final String VERSION_PLACEHOLDER = "PLACEHOLDER_FOR_VERSION";

    public enum VersionedTestPlugin {

        CONNECTOR("sampling-connector", "test.plugins.VersionedSamplingSinkConnector", "test.plugins.VersionedSamplingSourceConnector"),
        CONVERTER("sampling-converter", "test.plugins.VersionedSamplingConverter"),
        HEADER_CONVERTER("sampling-header-converter", "test.plugins.VersionedSamplingHeaderConverter"),
        TRANSFORMATION("versioned-transformation", "test.plugins.VersionedTransformation"),
        PREDICATE("versioned-predicate", "test.plugins.VersionedPredicate");

        private final String resourceDir;
        private final List<String> classNames;

        VersionedTestPlugin(String resourceDir, String... className) {
            this.resourceDir = resourceDir;
            this.classNames = Arrays.asList(className);
        }

        public String resourceDir() {
            return resourceDir;
        }

        public List<String> classNames() {
            return classNames;
        }
    }

    public static class BuildInfo {

        private final VersionedTestPlugin plugin;
        private final String version;
        private String location;

        private BuildInfo(VersionedTestPlugin plugin, String version) {
            this.plugin = plugin;
            this.version = version;
        }

        private void setLocation(String location) {
            this.location = location;
        }

        public VersionedTestPlugin plugin() {
            return plugin;
        }

        public String version() {
            return version;
        }

        public String location() {
            return location;
        }
    }

    private final List<BuildInfo> pluginBuilds;

    public VersionedPluginBuilder() {
        pluginBuilds = new ArrayList<>();
    }

    public VersionedPluginBuilder include(VersionedTestPlugin plugin, String version) {
        pluginBuilds.add(new BuildInfo(plugin, version));
        return this;
    }

    public synchronized Path build(String pluginDir) throws IOException {
        Path pluginDirPath = Files.createTempDirectory(pluginDir);
        Path subDir = Files.createDirectory(pluginDirPath.resolve("lib"));
        for (BuildInfo buildInfo : pluginBuilds) {
            Path jarFile = TestPlugins.createPluginJar(buildInfo.plugin.resourceDir(), ignored -> false, Collections.singletonMap(VERSION_PLACEHOLDER, buildInfo.version));
            Path targetJar = subDir.resolve(jarFile.getFileName()).toAbsolutePath();
            buildInfo.setLocation(targetJar.toString());
            Files.move(jarFile, targetJar);
        }
        return pluginDirPath.toAbsolutePath();
    }

    public List<BuildInfo> buildInfos() {
        return pluginBuilds;
    }
}
