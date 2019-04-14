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

package org.apache.kafka.connect.health;

import java.util.Collection;
import java.util.Map;

/**
 * Provides the ability to lookup connector metadata and its health. This is made available to the {@link org.apache.kafka.connect.rest.ConnectRestExtension}
 * implementations. The Connect framework provides the implementation for this interface.
 */
public interface ConnectClusterState {

    /**
     * Get the names of the connectors currently deployed in this cluster. This is a full list of connectors in the cluster gathered from
     * the current configuration, which may change over time.
     *
     * @return collection of connector names, never {@code null}
     */
    Collection<String> connectors();

    /**
     * Lookup the current health of a connector and its tasks. This provides the current snapshot of health by querying the underlying
     * herder. A connector returned by previous invocation of {@link #connectors()} may no longer be available and could result in {@link
     * org.apache.kafka.connect.errors.NotFoundException}.
     *
     * @param connName name of the connector
     * @return the health of the connector for the connector name
     * @throws org.apache.kafka.connect.errors.NotFoundException if the requested connector can't be found
     */
    ConnectorHealth connectorHealth(String connName);

    /**
     * Lookup the current configuration of a connector. This provides the current snapshot of configuration by querying the underlying
     * herder. A connector returned by previous invocation of {@link #connectors()} may no longer be available and could result in {@link
     * org.apache.kafka.connect.errors.NotFoundException}.
     *
     * @param connName name of the connector
     * @return the configuration of the connector for the connector name
     * @throws org.apache.kafka.connect.errors.NotFoundException if the requested connector can't be found
     */
    Map<String, String> connectorConfig(String connName);

    /**
     * Lookup the current task configurations of a connector. This provides the current snapshot of configuration by querying the underlying
     * herder. A connector returned by previous invocation of {@link #connectors()} may no longer be available and could result in {@link
     * org.apache.kafka.connect.errors.NotFoundException}.
     *
     * @param connName name of the connector
     * @return the configuration for each task ID
     * @throws org.apache.kafka.connect.errors.NotFoundException if the requested connector can't be found
     **/
    Map<Integer, Map<String, String>> taskConfigs(String connName);

    /**
     * Get the cluster ID of the Kafka cluster backing this Connect cluster.
     * @return the cluster ID of the Kafka cluster backing this connect cluster
     **/
    String kafkaClusterId();
}
