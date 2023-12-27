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

package org.apache.kafka.tools.config;

import joptsimple.OptionSpec;
import org.apache.kafka.common.config.ZkConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.DynamicConfig;
import org.apache.kafka.server.config.ConfigType;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.tools.reassign.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.utils.Utils.NL;

public class ConfigCommandOptions extends CommandDefaultOptions {
    public static final String BROKER_LOGGER_CONFIG_TYPE = "broker-loggers";
    public static final List<String> BROKER_SUPPORTED_CONFIG_TYPES;
    public static final List<String> ZK_SUPPORTED_CONFIG_TYPES = Arrays.asList(ConfigType.USER, ConfigType.BROKER);
    
    static {
        List<String> brokerSupportedConfigTypes = new ArrayList<>();

        brokerSupportedConfigTypes.addAll(ConfigType.ALL);
        brokerSupportedConfigTypes.add(BROKER_LOGGER_CONFIG_TYPE);
        brokerSupportedConfigTypes.add(ConfigType.CLIENT_METRICS);

        BROKER_SUPPORTED_CONFIG_TYPES = Collections.unmodifiableList(brokerSupportedConfigTypes);
    }

    final OptionSpec<String> zkConnectOpt;
    final OptionSpec<String> bootstrapServerOpt;
    final OptionSpec<String> bootstrapControllerOpt;
    final OptionSpec<String> commandConfigOpt;
    final OptionSpec<?> alterOpt;
    final OptionSpec<?> describeOpt;
    final OptionSpec<?> allOpt;
    final OptionSpec<String> entityType;
    final OptionSpec<String> entityName;
    final OptionSpec<?> entityDefault;
    final OptionSpec<String> addConfig;
    final OptionSpec<String> addConfigFile;
    final OptionSpec<String> deleteConfig;
    final OptionSpec<?> forceOpt;
    final OptionSpec<String> topic;
    final OptionSpec<String> client;
    final OptionSpec<?> clientDefaults;
    final OptionSpec<String> user;
    final OptionSpec<?> userDefaults;
    final OptionSpec<String> broker;
    final OptionSpec<?> brokerDefaults;
    final OptionSpec<String> brokerLogger;
    final OptionSpec<?> ipDefaults;
    final OptionSpec<String> ip;
    final OptionSpec<String> zkTlsConfigFile;

    final List<Tuple2<OptionSpec<?>, String>> entityFlags;
    final List<Tuple2<OptionSpec<?>, String>> entityDefaultsFlags;
    final List<String> entityTypes;
    final List<String> entityNames;

    public ConfigCommandOptions(String[] args) {
        super(args);

        zkConnectOpt = parser.accepts("zookeeper", "DEPRECATED. The connection string for the zookeeper connection in the form host:port. " +
                "Multiple URLS can be given to allow fail-over. Required when configuring SCRAM credentials for users or " +
                "dynamic broker configs when the relevant broker(s) are down. Not allowed otherwise.")
            .withRequiredArg()
            .describedAs("urls")
            .ofType(String.class);
        bootstrapServerOpt = parser.accepts("bootstrap-server", "The Kafka servers to connect to.")
            .withRequiredArg()
            .describedAs("server to connect to")
            .ofType(String.class);
        bootstrapControllerOpt = parser.accepts("bootstrap-controller", "The Kafka controllers to connect to.")
            .withRequiredArg()
            .describedAs("controller to connect to")
            .ofType(String.class);
        commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client. " +
                "This is used only with --bootstrap-server option for describing and altering broker configs.")
            .withRequiredArg()
            .describedAs("command config property file")
            .ofType(String.class);
        alterOpt = parser.accepts("alter", "Alter the configuration for the entity.");
        describeOpt = parser.accepts("describe", "List configs for the given entity.");
        allOpt = parser.accepts("all", "List all configs for the given topic, broker, or broker-logger entity (includes static configuration when the entity type is brokers)");

        entityType = parser.accepts("entity-type", "Type of entity (topics/clients/users/brokers/broker-loggers/ips/client-metrics)")
            .withRequiredArg()
            .ofType(String.class);
        entityName = parser.accepts("entity-name", "Name of entity (topic name/client id/user principal name/broker id/ip/client metrics)")
            .withRequiredArg()
            .ofType(String.class);
        entityDefault = parser.accepts("entity-default", "Default entity name for clients/users/brokers/ips (applies to corresponding entity type in command line)");

        Function<Collection<String>, String> joinNl =
            names -> names.stream().sorted().map(n -> "\t" + n).collect(Collectors.joining(NL, NL, NL));

        addConfig = parser.accepts("add-config", "Key Value pairs of configs to add. Square brackets can be used to group values which contain commas: 'k1=v1,k2=[v1,v2,v2],k3=v3'. The following is a list of valid configurations: " +
                "For entity-type '" + ConfigType.TOPIC + "': " + joinNl.apply(LogConfig.configNames()) +
                "For entity-type '" + ConfigType.BROKER + "': " + joinNl.apply(DynamicConfig.Broker.NAMES) +
                "For entity-type '" + ConfigType.USER + "': " + joinNl.apply(DynamicConfig.User.NAMES) +
                "For entity-type '" + ConfigType.CLIENT + "': " + joinNl.apply(DynamicConfig.Client.NAMES) +
                "For entity-type '" + ConfigType.IP + "': " + joinNl.apply(DynamicConfig.Ip.NAMES) +
                "For entity-type '" + ConfigType.CLIENT_METRICS + "': " + joinNl.apply(DynamicConfig.ClientMetrics.NAMES) +
                "Entity types '" + ConfigType.USER + "' and '" + ConfigType.CLIENT + "' may be specified together to update config for clients of a specific user.")
            .withRequiredArg()
            .ofType(String.class);
        addConfigFile = parser.accepts("add-config-file", "Path to a properties file with configs to add. See add-config for a list of valid configurations.")
            .withRequiredArg()
            .ofType(String.class);
        deleteConfig = parser.accepts("delete-config", "config keys to remove 'k1,k2'")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',');
        forceOpt = parser.accepts("force", "Suppress console prompts");
        topic = parser.accepts("topic", "The topic's name.")
            .withRequiredArg()
            .ofType(String.class);
        client = parser.accepts("client", "The client's ID.")
            .withRequiredArg()
            .ofType(String.class);
        clientDefaults = parser.accepts("client-defaults", "The config defaults for all clients.");
        user = parser.accepts("user", "The user's principal name.")
            .withRequiredArg()
            .ofType(String.class);
        userDefaults = parser.accepts("user-defaults", "The config defaults for all users.");
        broker = parser.accepts("broker", "The broker's ID.")
            .withRequiredArg()
            .ofType(String.class);
        brokerDefaults = parser.accepts("broker-defaults", "The config defaults for all brokers.");
        brokerLogger = parser.accepts("broker-logger", "The broker's ID for its logger config.")
            .withRequiredArg()
            .ofType(String.class);
        ipDefaults = parser.accepts("ip-defaults", "The config defaults for all IPs.");
        ip = parser.accepts("ip", "The IP address.")
            .withRequiredArg()
            .ofType(String.class);
        zkTlsConfigFile = parser.accepts("zk-tls-config-file",
                "Identifies the file where ZooKeeper client TLS connectivity properties are defined.  Any properties other than " +
                    ZkConfig.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.keySet().stream().sorted().collect(Collectors.joining(", ")) + " are ignored.")
            .withRequiredArg().describedAs("ZooKeeper TLS configuration").ofType(String.class);

        options = parser.parse(args);

        entityFlags = Arrays.asList(new Tuple2<>(topic, ConfigType.TOPIC),
            new Tuple2<>(client, ConfigType.CLIENT),
            new Tuple2<>(user, ConfigType.USER),
            new Tuple2<>(broker, ConfigType.BROKER),
            new Tuple2<>(brokerLogger, BROKER_LOGGER_CONFIG_TYPE),
            new Tuple2<>(ip, ConfigType.IP));

        entityDefaultsFlags = Arrays.asList(new Tuple2<>(clientDefaults, ConfigType.CLIENT),
            new Tuple2<>(userDefaults, ConfigType.USER),
            new Tuple2<>(brokerDefaults, ConfigType.BROKER),
            new Tuple2<>(ipDefaults, ConfigType.IP));

        List<String> entityTypes0 = new ArrayList<>(options.valuesOf(entityType));

        Stream.concat(entityFlags.stream(), entityDefaultsFlags.stream())
            .filter(entity -> options.has(entity.v1))
            .map(t -> t.v2)
            .forEach(entityTypes0::add);

        entityTypes = Collections.unmodifiableList(entityTypes0);

        Iterator<String> namesIterator = options.valuesOf(entityName).iterator();

        List<String> entityNames0 = new ArrayList<>();

        options.specs().stream()
            .filter(spec -> spec.options().contains("entity-name") || spec.options().contains("entity-default"))
            .map(spec -> spec.options().contains("entity-name") ? namesIterator.next() : "")
            .forEach(entityNames0::add);

        entityFlags.stream()
            .filter(entity -> options.has(entity.v1))
            .map(entity -> options.valueOf(entity.v1))
            .map(String::valueOf)
            .forEach(entityNames0::add);

        entityDefaultsFlags.stream()
            .filter(entity -> options.has(entity.v1))
            .map(t -> "")
            .forEach(entityNames0::add);

        entityNames = Collections.unmodifiableList(entityNames0);
    }

    public void checkArgs() {
        // should have exactly one action
        long actions = Stream.of(alterOpt, describeOpt).filter(options::has).count();
        if (actions != 1)
            CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --describe, --alter");
        // check required args
        CommandLineUtils.checkInvalidArgs(parser, options, alterOpt, describeOpt);
        CommandLineUtils.checkInvalidArgs(parser, options, describeOpt, alterOpt, addConfig, deleteConfig);

        Set<String> entityTypeVals = new HashSet<>(entityTypes);
        if (entityTypes.size() != entityTypeVals.size()) {
            List<String> diff = new ArrayList<>(entityTypes);
            entityTypeVals.forEach(diff::remove);

            throw new IllegalArgumentException("Duplicate entity type(s) specified: " + Utils.join(diff, ","));
        }

        List<String> allowedEntityTypes;
        String connectOptString;

        if (options.has(bootstrapServerOpt) || options.has(bootstrapControllerOpt)) {
            allowedEntityTypes = BROKER_SUPPORTED_CONFIG_TYPES;
            connectOptString = "--bootstrap-server or --bootstrap-controller";
        } else {
            allowedEntityTypes = ZK_SUPPORTED_CONFIG_TYPES;
            connectOptString = "--zookeeper";
        }

        entityTypeVals.forEach(entityTypeVal -> {
            if (!allowedEntityTypes.contains(entityTypeVal))
                throw new IllegalArgumentException("Invalid entity type " + entityTypeVal + ", the entity type must be one of " + Utils.join(allowedEntityTypes, ", ") + " with a " + connectOptString + " argument");
        });

        if (entityTypeVals.isEmpty())
            throw new IllegalArgumentException("At least one entity type must be specified");
        else if (entityTypeVals.size() > 1 && !entityTypeVals.equals(new HashSet<>(Arrays.asList(ConfigType.USER, ConfigType.CLIENT))))
            throw new IllegalArgumentException("Only '" + ConfigType.USER + "' and '" + ConfigType.CLIENT + "' entity types may be specified together");

        if ((options.has(entityName) || options.has(entityType) || options.has(entityDefault)) &&
            Stream.concat(entityFlags.stream(), entityDefaultsFlags.stream()).anyMatch(entity -> options.has(entity.v1)))
            throw new IllegalArgumentException("--entity-{type,name,default} should not be used in conjunction with specific entity flags");

        boolean hasEntityName = entityNames.stream().anyMatch(e -> !e.isEmpty());
        boolean hasEntityDefault = entityNames.stream().anyMatch(String::isEmpty);

        int numConnectOptions = (options.has(bootstrapServerOpt) ? 1 : 0) +
            (options.has(bootstrapControllerOpt) ? 1 : 0) +
            (options.has(zkConnectOpt) ? 1 : 0);
        if (numConnectOptions == 0)
            throw new IllegalArgumentException("One of the required --bootstrap-server, --boostrap-controller, or --zookeeper arguments must be specified");
        else if (numConnectOptions > 1)
            throw new IllegalArgumentException("Only one of --bootstrap-server, --boostrap-controller, and --zookeeper can be specified");

        if (options.has(allOpt) && options.has(zkConnectOpt)) {
            throw new IllegalArgumentException("--bootstrap-server must be specified for --all");
        }
        if (options.has(zkTlsConfigFile) && !options.has(zkConnectOpt)) {
            throw new IllegalArgumentException("Only the --zookeeper option can be used with the --zk-tls-config-file option.");
        }

        if (hasEntityName && (entityTypeVals.contains(ConfigType.BROKER) || entityTypeVals.contains(BROKER_LOGGER_CONFIG_TYPE))) {
            Stream.of(entityName, broker, brokerLogger).filter(o -> options.has(o)).map(o -> options.valueOf(o)).forEach(brokerId -> {
                try {
                    Integer.valueOf(brokerId);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("The entity name for " + entityTypeVals.iterator().next() + " must be a valid integer broker id, but it is: " + brokerId);
                }
            });
        }

        if (hasEntityName && entityTypeVals.contains(ConfigType.IP)) {
            Stream.of(entityName, ip).filter(options::has).map(options::valueOf).forEach(ipEntity -> {
                if (!DynamicConfig.Ip.isValidIpEntity(ipEntity))
                    throw new IllegalArgumentException("The entity name for " + entityTypeVals.iterator().next() + " must be a valid IP or resolvable host, but it is: " + ipEntity);
            });
        }

        if (options.has(describeOpt) && entityTypeVals.contains(BROKER_LOGGER_CONFIG_TYPE) && !hasEntityName)
            throw new IllegalArgumentException("an entity name must be specified with --describe of " + Utils.join(entityTypeVals, ","));

        if (options.has(alterOpt)) {
            if (entityTypeVals.contains(ConfigType.USER) ||
                entityTypeVals.contains(ConfigType.CLIENT) ||
                entityTypeVals.contains(ConfigType.BROKER) ||
                entityTypeVals.contains(ConfigType.IP)) {
                if (!hasEntityName && !hasEntityDefault)
                    throw new IllegalArgumentException("an entity-name or default entity must be specified with --alter of users, clients, brokers or ips");
            } else if (!hasEntityName)
                throw new IllegalArgumentException("an entity name must be specified with --alter of " + Utils.join(entityTypeVals, ","));

            boolean isAddConfigPresent = options.has(addConfig);
            boolean isAddConfigFilePresent = options.has(addConfigFile);
            boolean isDeleteConfigPresent = options.has(deleteConfig);

            if(isAddConfigPresent && isAddConfigFilePresent)
                throw new IllegalArgumentException("Only one of --add-config or --add-config-file must be specified");

            if(!isAddConfigPresent && !isAddConfigFilePresent && !isDeleteConfigPresent)
                throw new IllegalArgumentException("At least one of --add-config, --add-config-file, or --delete-config must be specified with --alter");
        }
    }
}