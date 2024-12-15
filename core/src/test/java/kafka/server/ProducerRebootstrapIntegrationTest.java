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
package kafka.server;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ReplicationConfigs;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(240)
@ExtendWith(ClusterTestExtensions.class)
@ClusterTestDefaults(
        types = {Type.KRAFT},
        brokers = 2,
        serverProperties = {
            @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true"),
            @ClusterConfigProperty(key = ReplicationConfigs.UNCLEAN_LEADER_ELECTION_INTERVAL_MS_CONFIG, value = "1"),
        }

)
public class ProducerRebootstrapIntegrationTest {
    final String topicName = "topic";
    final Map<Integer, List<Integer>> expectedReplicaAssignment = Map.of(0, List.of(0, 1), 1, List.of(0, 1));
    private static final int[] PORTS = choosePorts(2);
    
    static {
        // Set system properties that will be used to replace ${port0} placeholders
        System.setProperty("port0", String.valueOf(PORTS[0]));
        System.setProperty("port1", String.valueOf(PORTS[1]));
    }

    @Timeout(240)
    @ClusterTest(
        types = {Type.KRAFT},
        brokers = 2,
        serverProperties = {
            @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true"),
            @ClusterConfigProperty(key = ReplicationConfigs.UNCLEAN_LEADER_ELECTION_INTERVAL_MS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2")
        }
    )
    public void testRebootstrap(ClusterInstance cluster) throws Exception {
        boolean useRebootstrapTriggerMs = true;
        int part = 0;
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.putAll(clientConfig(useRebootstrapTriggerMs));


        try (Admin admin = cluster.admin();
            Producer<String, String> producer = cluster.producer(Utils.propsToMap(producerProps))) {

            Properties topicProps = new Properties();
            topicProps.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true");

            admin.createTopics(List.of(new NewTopic(topicName, 2, (short) 2).configs(Utils.propsToStringMap(topicProps)))).all().get();

            cluster.brokers().get(1).shutdown();
            cluster.brokers().get(1).awaitShutdown();

            // Only the server 0 is available for the producer during the bootstrap.
            RecordMetadata recordMetadata0 = producer.send(new ProducerRecord<>(topicName, part, "key 0", "value 0")).get();
            assertEquals(0, recordMetadata0.offset());

            cluster.brokers().get(0).shutdown();
            cluster.brokers().get(0).awaitShutdown();
            cluster.brokers().get(1).startup();

            // The server 0, originally cached during the bootstrap, is offline.
            // However, the server 1 from the bootstrap list is online.
            // Should be able to produce records.
            RecordMetadata recordMetadata1 = producer.send(new ProducerRecord<>(topicName, part, "key 1", "value 1")).get();
            assertEquals(0, recordMetadata1.offset());

            cluster.brokers().get(1).shutdown();
            cluster.brokers().get(1).awaitShutdown();
            cluster.brokers().get(0).startup();

            // The same situation, but the server 1 has gone and server 0 is back.
            RecordMetadata recordMetadata2 = producer.send(new ProducerRecord<>(topicName, part, "key 1", "value 1")).get();
            assertEquals(1, recordMetadata2.offset());
        }

        /* scala code:
        server1.shutdown();
        server1.awaitShutdown();

        val producer = createProducer(configOverrides = clientOverrides(useRebootstrapTriggerMs));

        // Only the server 0 is available for the producer during the bootstrap.
        val recordMetadata0 = producer.send(new ProducerRecord(topic, part, "key 0".getBytes, "value 0".getBytes)).get();
        assertEquals(0, recordMetadata0.offset());

        server0.shutdown();
        server0.awaitShutdown();
        server1.startup();

        // The server 0, originally cached during the bootstrap, is offline.
        // However, the server 1 from the bootstrap list is online.
        // Should be able to produce records.
        val recordMetadata1 = producer.send(new ProducerRecord(topic, part, "key 1".getBytes, "value 1".getBytes)).get();
        assertEquals(0, recordMetadata1.offset());

        server1.shutdown();
        server1.awaitShutdown();
        server0.startup();

        // The same situation, but the server 1 has gone and server 0 is back.
        val recordMetadata2 = producer.send(new ProducerRecord(topic, part, "key 1".getBytes, "value 1".getBytes)).get();
        assertEquals(1, recordMetadata2.offset());
         */
    }

    Properties clientConfig(boolean useRebootstrapTriggerMs) {
        Properties overrides = new Properties();
        if (useRebootstrapTriggerMs) {
            overrides.put(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "5000");
        } else {
            overrides.put(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "3600000");
            overrides.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "5000");
            overrides.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, "5000");
            overrides.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, "1000");
            overrides.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, "1000");
        }
        overrides.put(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "rebootstrap");
        return overrides;
    }

    private static int[] choosePorts(int count) {
        int[] ports = new int[count];
        List<ServerSocket> sockets = new ArrayList<>(count);
        try {
            // Create server sockets with system-assigned ports
            for (int i = 0; i < count; i++) {
                ServerSocket socket = new ServerSocket(0);
                sockets.add(socket);
                ports[i] = socket.getLocalPort();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to find available ports", e);
        } finally {
            // Close all sockets
            for (ServerSocket socket : sockets) {
                try {
                    socket.close();
                } catch (IOException e) {
                    // Ignore close exceptions
                }
            }
        }
        return ports;
    }

    // def choosePorts(count: Int): Seq[Int] = {
    //     try {
    //       val sockets = (0 until count).map(_ => new ServerSocket(0))
    //       val ports = sockets.map(_.getLocalPort())
    //       sockets.foreach(_.close())
    //       ports
    //     } catch {
    //       case e: IOException => throw new RuntimeException(e)
    //     }
    //   }
    // }

//     def createBrokerConfigs(numConfigs: Int,
//     zkConnect: String,
//     enableControlledShutdown: Boolean = true,
//     enableDeleteTopic: Boolean = false): Seq[Properties] = {
//     val ports = FixedPortTestUtils.choosePorts(numConfigs)
//     (0 until numConfigs).map { node =>
//       TestUtils.createBrokerConfig(node, zkConnect, enableControlledShutdown, enableDeleteTopic, ports(node))
//     }
//   }

//    def createBrokerConfig(nodeId: Int,
//                           zkConnect: String,
//                           enableControlledShutdown: Boolean = true,
//                           enableDeleteTopic: Boolean = true,
//                           port: Int = RandomPort,
//                           interBrokerSecurityProtocol: Option[SecurityProtocol] = None,
//                           trustStoreFile: Option[File] = None,
//                           saslProperties: Option[Properties] = None,
//                           enablePlaintext: Boolean = true,
//                           enableSaslPlaintext: Boolean = false,
//                           saslPlaintextPort: Int = RandomPort,
//                           enableSsl: Boolean = false,
//                           sslPort: Int = RandomPort,
//                           enableSaslSsl: Boolean = false,
//                           saslSslPort: Int = RandomPort,
//                           rack: Option[String] = None,
//                           logDirCount: Int = 1,
//                           enableToken: Boolean = false,
//                           numPartitions: Int = 1,
//                           defaultReplicationFactor: Short = 1,
//                           enableFetchFromFollower: Boolean = false): Properties = {
//        def shouldEnable(protocol: SecurityProtocol) = interBrokerSecurityProtocol.fold(false)(_ == protocol)
//
//        val protocolAndPorts = ArrayBuffer[(SecurityProtocol, Int)]()
//        if (enablePlaintext || shouldEnable(SecurityProtocol.PLAINTEXT))
//            protocolAndPorts += SecurityProtocol.PLAINTEXT -> port
//        if (enableSsl || shouldEnable(SecurityProtocol.SSL))
//            protocolAndPorts += SecurityProtocol.SSL -> sslPort
//        if (enableSaslPlaintext || shouldEnable(SecurityProtocol.SASL_PLAINTEXT))
//            protocolAndPorts += SecurityProtocol.SASL_PLAINTEXT -> saslPlaintextPort
//        if (enableSaslSsl || shouldEnable(SecurityProtocol.SASL_SSL))
//            protocolAndPorts += SecurityProtocol.SASL_SSL -> saslSslPort
//
//        val listeners = protocolAndPorts.map { case (protocol, port) =>
//            s"${protocol.name}://localhost:$port"
//        }.mkString(",")
//
//        val props = new Properties
//        props.put(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true")
//        props.put(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
//            props.setProperty(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG, TimeUnit.MINUTES.toMillis(10).toString)
//            props.put(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
//            props.put(ServerConfigs.BROKER_ID_CONFIG, nodeId.toString)
//            props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, listeners)
//            props.put(SocketServerConfigs.LISTENERS_CONFIG, listeners)
//            props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
//            props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, protocolAndPorts.
//                    map(p => "%s:%s".format(p._1, p._1)).mkString(",") + ",CONTROLLER:PLAINTEXT")
//        }
}
