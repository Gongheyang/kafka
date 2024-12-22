package org.apache.kafka.connect.runtime.isolation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.jar.JarOutputStream;

public class VersionedPluginBuilder {

    public static class VersionProvider {

        private static String version;

        protected static void setVersion(String version) {
            VersionProvider.version = version;
        }

        public static String getVersion(String version) {
            return VersionProvider.version;
        }
    }

    public enum VersionedTestPlugin {

        SOURCE_CONNECTOR("", ""),
        SINK_CONNECTOR("", ""),
        CONVERTER("", ""),
        HEADER_CONVERTER("", ""),
        TRANSFORMATION("", ""),
        PREDICATE("", "");

        private final String resourceDir;
        private final String className;

        VersionedTestPlugin(String resourceDir, String className) {
            this.resourceDir = resourceDir;
            this.className = className;
        }

        public String resourceDir() {
            return resourceDir;
        }

        public String className() {
            return className;
        }

    }

    private static class BuildInfo {

        private final VersionedTestPlugin plugin;
        private final String version;

        private BuildInfo(VersionedTestPlugin plugin, String version) {
            this.plugin = plugin;
            this.version = version;
        }

        public VersionedTestPlugin plugin() {
            return plugin;
        }

        public String version() {
            return version;
        }
    }

    private final List<BuildInfo> pluginBuilds;

    public VersionedPluginBuilder() {
        pluginBuilds = new ArrayList<>();
    }

    public void include(VersionedTestPlugin plugin, String version) {
        pluginBuilds.add(new BuildInfo(plugin, version));
    }

    public synchronized Path build(String pluginDir) throws IOException {
        Path pluginDirPath = Files.createTempDirectory(pluginDir);
        for (BuildInfo buildInfo : pluginBuilds) {
            VersionProvider.setVersion(buildInfo.version());
            Path jarFile = TestPlugins.createPluginJar(buildInfo.plugin.resourceDir(), className -> false);
            Path targetJar = pluginDirPath.resolve(jarFile.getFileName());
            Files.move(jarFile, targetJar);
        }
        return pluginDirPath;
    }
}
