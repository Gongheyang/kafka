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

import org.apache.maven.artifact.versioning.ArtifactVersion;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;
import org.apache.maven.artifact.versioning.VersionRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * A custom classloader dedicated to loading Connect plugin classes in classloading isolation.
 *
 * <p>
 * Under the current scheme for classloading isolation in Connect, the delegating classloader loads
 * plugin classes that it finds in its child plugin classloaders. For classes that are not plugins,
 * this delegating classloader delegates its loading to its parent. This makes this classloader a
 * child-first classloader.
 * <p>
 * This class is thread-safe and parallel capable.
 */
public class DelegatingClassLoader extends URLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(DelegatingClassLoader.class);

    private final ConcurrentMap<String, SortedMap<PluginDesc<?>, ClassLoader>> pluginLoaders;
    private final ConcurrentMap<String, String> aliases;

    // Although this classloader does not load classes directly but rather delegates loading to a
    // PluginClassLoader or its parent through its base class, because of the use of inheritance
    // in the latter case, this classloader needs to also be declared as parallel capable to use
    // fine-grain locking when loading classes.
    static {
        ClassLoader.registerAsParallelCapable();
    }

    public DelegatingClassLoader(ClassLoader parent) {
        super(new URL[0], parent);
        this.pluginLoaders = new ConcurrentHashMap<>();
        this.aliases = new ConcurrentHashMap<>();
    }

    public DelegatingClassLoader() {
        // Use as parent the classloader that loaded this class. In most cases this will be the
        // System classloader. But this choice here provides additional flexibility in managed
        // environments that control classloading differently (OSGi, Spring and others) and don't
        // depend on the System classloader to load Connect's classes.
        this(DelegatingClassLoader.class.getClassLoader());
    }

    /**
     * Retrieve the PluginClassLoader associated with a plugin class
     *
     * @param name The fully qualified class name of the plugin
     * @return the PluginClassLoader that should be used to load this, or null if the plugin is not isolated.
     */
    // VisibleForTesting
    PluginClassLoader pluginClassLoader(String name, VersionRange range) {
        if (!PluginUtils.shouldLoadInIsolation(name)) {
            return null;
        }

        SortedMap<PluginDesc<?>, ClassLoader> inner = pluginLoaders.get(name);
        if (inner == null) {
            return null;
        }


        ClassLoader pluginLoader = findPluginLoader(inner, name, range);
        return pluginLoader instanceof PluginClassLoader
            ? (PluginClassLoader) pluginLoader
            : null;
    }

    PluginClassLoader pluginClassLoader(String name) {
        return pluginClassLoader(name, null);
    }

    ClassLoader connectorLoader(String connectorClassOrAlias, VersionRange range) throws ClassNotFoundException {
        String fullName = aliases.getOrDefault(connectorClassOrAlias, connectorClassOrAlias);
        // if the plugin is not loaded via the plugin classloader, it might still be available in the parent delegating
        // classloader, in order to check if the version satisfies the requirement we need to load the plugin class here
        ClassLoader classLoader = loadVersionedPluginClass(fullName, range, false).getClassLoader();
        log.debug(
                "Got plugin class loader: '{}' for connector: {}",
                classLoader,
                connectorClassOrAlias
        );
        return classLoader;
    }

    ClassLoader connectorLoader(String connectorClassOrAlias) {
        String fullName = aliases.getOrDefault(connectorClassOrAlias, connectorClassOrAlias);
        ClassLoader classLoader = pluginClassLoader(fullName);
        if (classLoader == null) classLoader = this;
        log.debug(
                "Getting plugin class loader: '{}' for connector: {}",
                classLoader,
                connectorClassOrAlias
        );
        return classLoader;
    }

    String resolveFullClassName(String classOrAlias) {
        return aliases.getOrDefault(classOrAlias, classOrAlias);
    }

    String latestVersion(String classOrAlias) {
        if (classOrAlias == null) {
            return null;
        }
        String fullName = aliases.getOrDefault(classOrAlias, classOrAlias);
        SortedMap<PluginDesc<?>, ClassLoader> inner = pluginLoaders.get(fullName);
        if (inner == null) {
            return null;
        }
        return inner.lastKey().version();
    }

    private ClassLoader findPluginLoader(
        SortedMap<PluginDesc<?>, ClassLoader> loaders,
        String pluginName,
        VersionRange range
    ) {

        if (range != null) {

            if (null != range.getRecommendedVersion()) {
                throw new VersionedPluginLoadingException(String.format("A soft version range is not supported for plugin loading, "
                        + "this is an internal error as connect should automatically convert soft ranges to hard ranges. "
                        + "Provided soft version: %s ", range));
            }

            ArtifactVersion version = null;
            ClassLoader loader = null;
            for (Map.Entry<PluginDesc<?>, ClassLoader> entry : loaders.entrySet()) {
                // the entries should be in sorted order of versions so this should end up picking the latest version which matches the range
                if (range.containsVersion(entry.getKey().encodedVersion())) {
                    version = entry.getKey().encodedVersion();
                    loader = entry.getValue();
                }
            }

            if (version == null) {
                List<String> availableVersions = loaders.keySet().stream().map(PluginDesc::version).collect(Collectors.toList());
                throw new VersionedPluginLoadingException(String.format(
                        "Plugin loader for %s not found that matches the version range %s, available versions: %s",
                        pluginName,
                        range,
                        availableVersions
                ), availableVersions);
            }
            return loader;
        }

        return loaders.get(loaders.lastKey());
    }

    public void installDiscoveredPlugins(PluginScanResult scanResult) {
        pluginLoaders.putAll(computePluginLoaders(scanResult));
        for (String pluginClassName : pluginLoaders.keySet()) {
            log.info("Added plugin '{}'", pluginClassName);
        }
        aliases.putAll(PluginUtils.computeAliases(scanResult));
        for (Map.Entry<String, String> alias : aliases.entrySet()) {
            log.info("Added alias '{}' to plugin '{}'", alias.getKey(), alias.getValue());
        }
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        return loadVersionedPluginClass(name, null, resolve);
    }

    protected Class<?> loadVersionedPluginClass(
        String name,
        VersionRange range,
        boolean resolve
    ) throws VersionedPluginLoadingException, ClassNotFoundException {

        String fullName = aliases.getOrDefault(name, name);
        PluginClassLoader pluginLoader = pluginClassLoader(fullName, range);
        Class<?> plugin;
        if (pluginLoader != null) {
            log.trace("Retrieving loaded class '{}' from '{}'", name, pluginLoader);
            plugin = pluginLoader.loadClass(fullName, resolve);
        } else {
            plugin = super.loadClass(fullName, resolve);
            if (range == null) {
                return plugin;
            }
            // if we are loading a plugin class from the parent classloader, we need to check if the version
            // matches the range
            String pluginVersion;
            try (LoaderSwap classLoader = PluginScanner.withClassLoader(plugin.getClassLoader())) {
                pluginVersion = PluginScanner.versionFor(plugin.getDeclaredConstructor().newInstance());
            } catch (ReflectiveOperationException | LinkageError e) {
                throw new VersionedPluginLoadingException(String.format(
                        "Plugin %s was loaded with %s but failed to determine its version",
                        name,
                        plugin.getClassLoader()
                ), e);
            }

            if (!range.containsVersion(new DefaultArtifactVersion(pluginVersion))) {
                throw new VersionedPluginLoadingException(String.format(
                        "Plugin %s has version %s which does not match the required version range %s",
                        name,
                        pluginVersion,
                        range
                ), Collections.singletonList(pluginVersion));
            }
        }


        return plugin;
    }



    private static Map<String, SortedMap<PluginDesc<?>, ClassLoader>> computePluginLoaders(PluginScanResult plugins) {
        Map<String, SortedMap<PluginDesc<?>, ClassLoader>> pluginLoaders = new HashMap<>();
        plugins.forEach(pluginDesc ->
            pluginLoaders.computeIfAbsent(pluginDesc.className(), k -> new TreeMap<>())
                .put(pluginDesc, pluginDesc.loader()));
        return pluginLoaders;
    }
}
