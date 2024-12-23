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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class MultiVersionTest {

    private Plugins setUpPlugins(Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts, PluginDiscoveryMode mode) {
        String pluginPath = artifacts.keySet().stream().map(Path::toString).collect(Collectors.joining(","));
        Map<String, String> configs = new HashMap<>();
        configs.put(WorkerConfig.PLUGIN_PATH_CONFIG, pluginPath);
        configs.put(WorkerConfig.PLUGIN_DISCOVERY_CONFIG, mode.name());
        return new Plugins(configs);
    }

    private void assertPluginLoad(Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts, PluginDiscoveryMode mode)
            throws InvalidVersionSpecificationException, ClassNotFoundException {

        Plugins plugins = setUpPlugins(artifacts, mode);

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

    private static final PluginType[] ALL_PLUGIN_TYPES = new PluginType[]{
        PluginType.SINK, PluginType.SOURCE, PluginType.CONVERTER,
        PluginType.HEADER_CONVERTER, PluginType.TRANSFORMATION, PluginType.PREDICATE
    };

    private void assertCorrectLatestPluginVersion(
            Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts,
            PluginDiscoveryMode mode,
            String latestVersion
    ) {
        Plugins plugins = setUpPlugins(artifacts, mode);
        List<String> classes = artifacts.values().stream()
                .flatMap(List::stream)
                .map(VersionedPluginBuilder.BuildInfo::plugin)
                .flatMap(p -> p.classNames().stream())
                .distinct()
                .toList();
        for (String className : classes) {
            String version = plugins.latestVersion(className, ALL_PLUGIN_TYPES);
            Assertions.assertEquals(latestVersion, version);
        }
    }

    private static Map<Path, List<VersionedPluginBuilder.BuildInfo>> buildIsolatedArtifacts(
            String[] versions,
            VersionedPluginBuilder.VersionedTestPlugin[] pluginTypes
    ) throws IOException {
        Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts = new HashMap<>();
        for (String v : versions) {
            for (VersionedPluginBuilder.VersionedTestPlugin pluginType: pluginTypes) {
                VersionedPluginBuilder builder = new VersionedPluginBuilder();
                builder.include(pluginType, v);
                artifacts.put(builder.build(pluginType + "-" + v), builder.buildInfos());
            }
        }
        return artifacts;
    }

    private static String defaultIsolatedArtifactsLatestVersion;
    private static Map<Path, List<VersionedPluginBuilder.BuildInfo>> defaultIsolatedArtifacts;
    private static Map<Path, List<VersionedPluginBuilder.BuildInfo>> defaultCombinedArtifact;

    @BeforeAll
    public static void setUp() throws IOException {

        String[] defaultIsolatedArtifactsVersions = new String[]{"1.1.0", "2.3.0", "4.3.0"};
        defaultIsolatedArtifacts = buildIsolatedArtifacts(
                defaultIsolatedArtifactsVersions, VersionedPluginBuilder.VersionedTestPlugin.values()
        );
        defaultIsolatedArtifactsLatestVersion = "4.3.0";

        VersionedPluginBuilder builder = new VersionedPluginBuilder();
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.CONNECTOR, "0.1.0");
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.CONVERTER, "0.2.0");
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER, "0.3.0");
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION, "0.4.0");
        builder.include(VersionedPluginBuilder.VersionedTestPlugin.PREDICATE, "0.5.0");
        defaultCombinedArtifact = Collections.singletonMap(builder.build("all_versioned_artifact"), builder.buildInfos());
    }

    @Test
    public void TestVersionedPluginLoaded() throws IOException, InvalidVersionSpecificationException, ClassNotFoundException {
        assertPluginLoad(defaultCombinedArtifact, PluginDiscoveryMode.SERVICE_LOAD);
        assertPluginLoad(defaultCombinedArtifact, PluginDiscoveryMode.ONLY_SCAN);
    }

    @Test
    public void TestMultipleIsolatedVersionedPluginLoading() throws InvalidVersionSpecificationException, ClassNotFoundException {
        assertPluginLoad(defaultIsolatedArtifacts, PluginDiscoveryMode.SERVICE_LOAD);
        assertPluginLoad(defaultIsolatedArtifacts, PluginDiscoveryMode.ONLY_SCAN);
    }

    @Test
    public void TestLatestVersion() {
        assertCorrectLatestPluginVersion(defaultIsolatedArtifacts, PluginDiscoveryMode.SERVICE_LOAD, defaultIsolatedArtifactsLatestVersion);
        assertCorrectLatestPluginVersion(defaultIsolatedArtifacts, PluginDiscoveryMode.ONLY_SCAN, defaultIsolatedArtifactsLatestVersion);
    }

    @Test
    public void TestBundledPluginLoading() throws InvalidVersionSpecificationException, ClassNotFoundException {
        Map<Path, List<VersionedPluginBuilder.BuildInfo>> artifacts = new HashMap<>();
        artifacts.putAll(defaultCombinedArtifact);
        artifacts.putAll(defaultIsolatedArtifacts);

        Plugins plugins = setUpPlugins(artifacts, PluginDiscoveryMode.SERVICE_LOAD);
        // get the connector loader of the combined artifact which includes all plugin types
        ClassLoader connectorLoader = plugins.pluginLoader(
            VersionedPluginBuilder.VersionedTestPlugin.CONNECTOR.classNames().get(0),
            PluginUtils.connectorVersionRequirement("0.1.0")
        );
        Assertions.assertInstanceOf(PluginClassLoader.class, connectorLoader);

        List<VersionedPluginBuilder.VersionedTestPlugin> pluginTypes = List.of(
            VersionedPluginBuilder.VersionedTestPlugin.CONVERTER,
            VersionedPluginBuilder.VersionedTestPlugin.HEADER_CONVERTER,
            VersionedPluginBuilder.VersionedTestPlugin.TRANSFORMATION,
            VersionedPluginBuilder.VersionedTestPlugin.PREDICATE
        );
        // should match the version used in setUp for creating the combined artifact
        List<String> versions = Arrays.asList("0.2.0", "0.3.0", "0.4.0", "0.5.0");
        for (int i = 0; i < 4; i++) {
            String className = pluginTypes.get(i).classNames().get(0);
            // when using the connector loader, the version and plugin returned should be from the ones in the combined artifact
            String version = plugins.pluginVersion(className, connectorLoader, ALL_PLUGIN_TYPES);
            Assertions.assertEquals(versions.get(i), version);
            Object p = plugins.newPlugin(className, null, connectorLoader);
            Assertions.assertInstanceOf(Versioned.class, p);
            Assertions.assertEquals(versions.get(i), ((Versioned) p).version());
        }
    }
}
