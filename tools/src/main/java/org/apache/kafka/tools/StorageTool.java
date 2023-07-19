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
package org.apache.kafka.tools;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.bootstrap.BootstrapDirectory;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.broker.BrokerMetadataCheckpoint;
import org.apache.kafka.metadata.broker.MetaProperties;
import org.apache.kafka.metadata.broker.RawMetaProperties;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class StorageTool {
    public static void main(String... args) {
        try {
            Namespace namespace = parseArguments(args);
            String command = namespace.getString("command");
            Optional<LogConfig> config = Optional.ofNullable(namespace.getString("config")).map(p -> {
                try {
                    return new LogConfig(Utils.loadProps(p));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            executeCommand(namespace, command, config);
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
            Exit.exit(1);
        }
    }

    private static void executeCommand(Namespace namespace, String command, Optional<LogConfig> config) throws Exception {
        final String info = "info";
        final String format = "format";
        if ((command.equals(info) || command.equals(format)) && !config.isPresent()) {
            return; // Do nothing if config is not present
        }

        switch (command) {
            case info: {
                List<String> directories = configToLogDirectories(config.get());
                boolean selfManagedMode = configToSelfManagedMode(config.get());
                Exit.exit(infoCommand(System.out, selfManagedMode, directories));
                break;
            }
            case format: {
                List<String> directories = configToLogDirectories(config.get());
                String clusterId = namespace.getString("cluster_id");
                MetadataVersion metadataVersion = getMetadataVersion(namespace, Optional.of(config.get().getInterBrokerProtocolVersionString()));
                if (!metadataVersion.isKRaftSupported()) {
                    throw new TerseException("Must specify a valid KRaft metadata version of at least 3.0.");
                }
                MetaProperties metaProperties = buildMetadataProperties(clusterId, config.get());
                Boolean ignoreFormatted = namespace.getBoolean("ignore_formatted");
                if (!configToSelfManagedMode(config.get())) {
                    throw new TerseException("The kafka configuration file appears to be for " +
                        "a legacy cluster. Formatting is only supported for clusters in KRaft mode.");
                }
                Exit.exit(formatCommand(System.out, directories, metaProperties, metadataVersion, ignoreFormatted));
                break;
            }
            case "random-uuid": {
                System.out.println(Uuid.randomUuid());
                Exit.exit(0);
                break;
            }
            default:
                throw new RuntimeException("Unknown command " + command);
        }
    }

    static int infoCommand(PrintStream stream, boolean selfManagedMode, List<String> directories) throws IOException {
        List<String> problems = new ArrayList<>();
        List<String> foundDirectories = new ArrayList<>();
        Optional<RawMetaProperties> prevMetadata = Optional.empty();
        for (String directory : directories) {
            Path directoryPath = Paths.get(directory);
            if (!Files.isDirectory(directoryPath)) {
                if (!Files.exists(directoryPath)) {
                    problems.add(directoryPath + " does not exist");
                } else {
                    problems.add(directoryPath + " is not a directory");
                }
            } else {
                foundDirectories.add(directoryPath.toString());
                Path metaPath = directoryPath.resolve("meta.properties");
                if (!Files.exists(metaPath)) {
                    problems.add(directoryPath + " is not formatted.");
                } else {
                    Properties properties = Utils.loadProps(metaPath.toString());
                    RawMetaProperties rawMetaProperties = new RawMetaProperties(properties);
                    Optional<RawMetaProperties> curMetadata;

                    switch (rawMetaProperties.getVersion()) {
                        case 0:
                        case 1:
                            curMetadata = Optional.of(rawMetaProperties);
                            break;
                        default:
                            problems.add("Unsupported version for " + metaPath + ": " + rawMetaProperties.getVersion());
                            curMetadata = Optional.empty();
                            break;
                    }

                    if (!prevMetadata.isPresent()) {
                        prevMetadata = curMetadata;
                    } else {
                        if (curMetadata.isPresent() && !prevMetadata.get().equals(curMetadata.get())) {
                            problems.add(String.format("Metadata for %s was %s, but other directories featured %s", metaPath, curMetadata.get(), prevMetadata.get()));
                        }
                    }
                }
            }
        }

        if (prevMetadata.isPresent()) {
            if (selfManagedMode) {
                if (prevMetadata.get().getVersion() == 0) {
                    problems.add("The kafka configuration file appears to be for a cluster in KRaft mode, but " + "the directories are formatted for legacy mode.");
                }
            } else if (prevMetadata.get().getVersion() == 1) {
                problems.add("The kafka configuration file appears to be for a legacy cluster, but " + "the directories are formatted for a cluster in KRaft mode.");
            }
        }

        return validateDirectories(stream, directories, problems, foundDirectories, prevMetadata);
    }

    private static int validateDirectories(PrintStream stream, List<String> directories, List<String> problems, List<String> foundDirectories, Optional<RawMetaProperties> prevMetadata) {
        if (directories.isEmpty()) {
            stream.println("No directories specified.");
            return 0;
        } else {
            if (!foundDirectories.isEmpty()) {
                if (foundDirectories.size() == 1) {
                    stream.println("Found log directory:");
                } else {
                    stream.println("Found log directories:");
                }
                foundDirectories.forEach(d -> stream.printf(d + "%n"));
                stream.println("");
            }

            if (prevMetadata.isPresent()) {
                RawMetaProperties prev = prevMetadata.get();
                stream.println("Found metadata: " + prev);
                stream.println("");
            }

            if (!problems.isEmpty()) {
                if (problems.size() == 1) {
                    stream.println("Found problem:");
                } else {
                    stream.println("Found problems:");
                }
                problems.forEach(d -> stream.printf(d + "%n"));
                stream.println("");
                return 1;
            } else {
                return 0;
            }
        }
    }


    static Namespace parseArguments(String... args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("kafka-storage").defaultHelp(true).description("The Kafka storage tool.");
        Subparsers subparsers = parser.addSubparsers().dest("command");
        Subparser infoParser = subparsers.addParser("info").help("Get information about the Kafka log directories on this node.");
        Subparser formatParser = subparsers.addParser("format").help("Format the Kafka log directories on this node.");
        subparsers.addParser("random-uuid").help("Print a random UUID.");

        for (Subparser subpparser : Arrays.asList(infoParser, formatParser)) {
            subpparser.addArgument("--config", "-c").action(store()).required(true).help("The Kafka configuration file to use.");
        }

        formatParser.addArgument("--cluster-id", "-t").action(store()).required(true).help("The cluster ID to use.");
        formatParser.addArgument("--ignore-formatted", "-g").action(storeTrue());
        formatParser.addArgument("--release-version", "-r").action(store()).help(String.format("A KRaft release version to use for the initial metadata version. The minimum is 3.0, the default is %s", MetadataVersion.latest().version()));

        return parser.parseArgsOrFail(args);
    }

    static List<String> configToLogDirectories(LogConfig logConfig) {
        List<String> logDirs = logConfig.getLogDirs();
        SortedSet<String> directories = new TreeSet<>(logDirs);
        String metadataLogDir = logConfig.getMetadataLogDir();
        if (metadataLogDir != null) {
            directories.add(metadataLogDir);
        }
        return new ArrayList<>(directories);
    }

    static boolean configToSelfManagedMode(LogConfig logConfig) {
        return !logConfig.parseProcessRoles().isEmpty();
    }


    static MetadataVersion getMetadataVersion(Namespace namespace, Optional<String> defaultVersionString) {
        MetadataVersion defaultValue;
        if (defaultVersionString != null && defaultVersionString.isPresent()) {
            defaultValue = MetadataVersion.fromVersionString(defaultVersionString.get());
        } else {
            defaultValue = MetadataVersion.latest();
        }
        String releaseVersion = namespace.getString("release_version");
        if (releaseVersion != null) {
            return MetadataVersion.fromVersionString(releaseVersion);
        } else {
            return defaultValue;
        }
    }

    static MetaProperties buildMetadataProperties(String clusterIdStr, LogConfig config) throws TerseException {
        Uuid effectiveClusterId;
        try {
            effectiveClusterId = Uuid.fromString(clusterIdStr);
        } catch (Throwable e) {
            throw new TerseException("Cluster ID string " + clusterIdStr + " does not appear to be a valid UUID: " + e.getMessage());
        }

        if (config.getNodeId() < 0) {
            throw new TerseException("The node.id must be set to a non-negative integer. We saw " + config.getNodeId());
        }

        return new MetaProperties(effectiveClusterId.toString(), config.getNodeId());
    }

    static int formatCommand(PrintStream stream, List<String> directories, MetaProperties metaProperties, MetadataVersion metadataVersion, boolean ignoreFormatted) throws TerseException {
        if (directories.isEmpty()) {
            throw new TerseException("No log directories found in the configuration.");
        }
        List<String> unformattedDirectories = directories.stream().filter(directory -> {
            if (!Files.isDirectory(Paths.get(directory)) || !Files.exists(Paths.get(directory, "meta.properties"))) {
                return true;
            } else if (!ignoreFormatted) {
                try {
                    throw new TerseException("Log directory " + directory + " is already formatted. " +
                        "Use --ignore-formatted to ignore this directory and format the others.");
                } catch (TerseException e) {
                    throw new RuntimeException(e.getMessage());
                }
            } else {
                return false;
            }
        }).collect(Collectors.toList());

        if (unformattedDirectories.isEmpty()) {
            stream.println("All of the log directories are already formatted.");
        }
        unformattedDirectories.forEach(directory -> {
            try {
                Files.createDirectories(Paths.get(directory));
            } catch (Exception e) {
                try {
                    throw new TerseException("Unable to create storage directory " + directory + ": " + e.getMessage());
                } catch (TerseException ex) {
                    throw new RuntimeException(ex);
                }
            }

            Path metaPropertiesPath = Paths.get(directory, "meta.properties");
            BrokerMetadataCheckpoint checkpoint = new BrokerMetadataCheckpoint(metaPropertiesPath.toFile());
            try {
                checkpoint.write(metaProperties.toProperties());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            BootstrapMetadata bootstrapMetadata = BootstrapMetadata.fromVersion(metadataVersion, "format command");
            BootstrapDirectory bootstrapDirectory = new BootstrapDirectory(directory, Optional.empty());
            try {
                bootstrapDirectory.writeBinaryFile(bootstrapMetadata);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            stream.println("Formatting " + directory + " with metadata.version " + metadataVersion + ".");
        });
        return 0;
    }
}
