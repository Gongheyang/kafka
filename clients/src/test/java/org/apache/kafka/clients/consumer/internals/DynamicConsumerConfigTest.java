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
package org.apache.kafka.clients.consumer.internals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class DynamicConsumerConfigTest {

    private MockTime time = new MockTime();
    private final LogContext logCtx = new LogContext();
    private ConsumerMetadata metadata = new ConsumerMetadata(0, 
            Long.MAX_VALUE, 
            false, 
            false, 
            new SubscriptionState(logCtx, OffsetResetStrategy.NONE), 
            logCtx, 
            new ClusterResourceListeners());
    private MockClient client = new MockClient(time, metadata);
    private ConsumerNetworkClient consumerClient;
    private DynamicConsumerConfig dynamicConfigs;
    private GroupRebalanceConfig rebalanceConfig;
    Properties props = new Properties();

    @Before
    public void setup() {
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        client.updateMetadata(TestUtils.metadataUpdateWith(1, Collections.singletonMap("test", 1)));
        
        consumerClient = new ConsumerNetworkClient(logCtx, client, metadata, time,
                100, 1000, Integer.MAX_VALUE);
        rebalanceConfig = new GroupRebalanceConfig(new ConsumerConfig(props), GroupRebalanceConfig.ProtocolType.CONSUMER);
        dynamicConfigs = new DynamicConsumerConfig(consumerClient, this, rebalanceConfig, time, logCtx);
    }


    @Test
    public void testPeriodicFetch() {
        // Send DescribeConfigsRequest
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
    
        // Respond to trigger callback
        client.prepareResponse(describeConfigsResponse(Errors.NONE));
        consumerClient.poll(time.timer(0));
       
        // Advance clock to before the dynamic config expiration
        time.sleep(15000);
        
        // Not ready to send another request because current configs haven't expired
        assertFalse(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));

        // Advance clock to after expiration
        time.sleep(20000);

        // Now another request should be sent
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
        assertFalse(dynamicConfigs.shouldDisable());
    }

    @Test
    public void testInitialAndPeriodicFetch() {
        // Send first DescribeConfigsRequest
        RequestFuture<ClientResponse> configsFuture = dynamicConfigs.maybeFetchInitialConfigs();
        // Respond to trigger callback
        client.prepareResponse(describeConfigsResponse(Errors.NONE));

        // The client will block on the future in this method call
        assertTrue(dynamicConfigs.maybeWaitForInitialConfigs(configsFuture));

        // Check that subsequest calls for initial configs don't do anything since this
        // should only be taking place before the first join group request
        configsFuture = dynamicConfigs.maybeFetchInitialConfigs();
        assertEquals(null, configsFuture);
        assertFalse(dynamicConfigs.maybeWaitForInitialConfigs(configsFuture));
       
        // Advance clock to before the dynamic config expiration
        time.sleep(15000);
        
        // Not ready to send another request because current configs haven't expired
        assertFalse(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));

        // Advance clock to after expiration
        time.sleep(20000);

        // Make sure that initial configs don't get fetched since they already did
        assertEquals(null, dynamicConfigs.maybeFetchInitialConfigs());

        // Now another request should be sent without blocking
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
        assertFalse(dynamicConfigs.shouldDisable());
    }

    @Test
    public void testShouldDisableWithInvalidRequestErrorCode() {
        // Send first DescribeConfigsRequest
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
    
        // Respond with invalid request error code to trigger callback
        client.prepareResponse(describeConfigsResponse(Errors.INVALID_REQUEST));
        consumerClient.poll(time.timer(0));
       
        // Make sure that this is setting itself to be disabled with INVALID_REQUEST code
        assertTrue(dynamicConfigs.shouldDisable());
    }

    @Test
    public void testInvalidTimeoutAndHeartbeatConfig() {
        // Send first DescribeConfigsRequest
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));

        Map<String, String> configs = new HashMap<>();

        // session.timeout.ms defaults to 10k and heartbeat.interval.ms defaults to 3k
        // the configuration below will be rejected since heartbeat.interval.ms >= session.timeout.ms
        configs.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, "9000");
        configs.put(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG, "9500");
    
        // Respond to trigger callback
        client.prepareResponse(describeConfigsResponse(configs, Errors.NONE));
        consumerClient.poll(time.timer(0));

        // Check that static default configs persisted instead of the invalid dynamic configs
        assertEquals(10000, rebalanceConfig.getSessionTimout());
        assertEquals(3000, rebalanceConfig.getHeartbeatInterval());
       
        // Advance clock to before the dynamic config expiration
        time.sleep(15000);
        
        // Not ready to send another request because current configs haven't expired
        assertFalse(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));

        // Advance clock to after expiration
        time.sleep(20000);

        // Now another request should be sent
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
        assertFalse(dynamicConfigs.shouldDisable());
    }

    @Test
    public void testValidTimeoutAndHeartbeatConfig() {
        // Send first DescribeConfigsRequest
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));

        Map<String, String> configs = new HashMap<>();

        // session.timeout.ms defaults to 10k and heartbeat.interval.ms defaults to 3k
        // the configuration below will be accepted since heartbeat.interval.ms < session.timeout.ms
        configs.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, "12000");
        configs.put(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG, "4000");
    
        // Respond to trigger callback
        client.prepareResponse(describeConfigsResponse(configs, Errors.NONE));
        consumerClient.poll(time.timer(0));

        // Check that static default configs persisted instead of the invalid dynamic configs
        assertEquals(12000, rebalanceConfig.getSessionTimout());
        assertEquals(4000, rebalanceConfig.getHeartbeatInterval());
       
        // Advance clock to before the dynamic config expiration
        time.sleep(15000);
        
        // Not ready to send another request because current configs haven't expired
        assertFalse(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));

        // Advance clock to after expiration
        time.sleep(20000);

        // Now another request should be sent
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
        assertFalse(dynamicConfigs.shouldDisable());
    }

    @Test
    public void testRevertToStaticConfigIfDynamicConfigPairMissing() {
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));

        Map<String, String> configs = new HashMap<>();

        // session.timeout.ms defaults to 10k and heartbeat.interval.ms defaults to 3k
        // the configuration below will be rejected since heartbeat.interval.ms >= session.timeout.ms
        configs.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, "11000");
        configs.put(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG, "4000");
    
        // Respond to trigger callback
        client.prepareResponse(describeConfigsResponse(configs, Errors.NONE));
        consumerClient.poll(time.timer(0));

        // Check that the dynamic configs were set
        assertEquals(11000, rebalanceConfig.getSessionTimout());
        assertEquals(4000, rebalanceConfig.getHeartbeatInterval());
       
        // Advance clock to before the dynamic config expiration
        time.sleep(15000);
        
        // Not ready to send another request because current configs haven't expired
        assertFalse(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));

        // Advance clock to after expiration
        time.sleep(20000);

        // Now another request should be sent
        assertTrue(dynamicConfigs.maybeFetchConfigs(time.milliseconds()));
        assertFalse(dynamicConfigs.shouldDisable());

        // Respond to trigger callback with no dynamic configs
        client.prepareResponse(describeConfigsResponse(new HashMap<>(), Errors.NONE));
        consumerClient.poll(time.timer(0));

        // Check that this was set back to static configs since no dynamic configs were present
        assertEquals(10000, rebalanceConfig.getSessionTimout());
        assertEquals(3000, rebalanceConfig.getHeartbeatInterval());
    }

    public DescribeConfigsResponse describeConfigsResponse(Errors error) {
        return describeConfigsResponse(Collections.emptyMap(), error);
    }

    public DescribeConfigsResponse describeConfigsResponse(Map<String, String> configs, Errors error) {
        List<DescribeConfigsResponseData.DescribeConfigsResult> results = new ArrayList<DescribeConfigsResponseData.DescribeConfigsResult>();
        DescribeConfigsResponseData.DescribeConfigsResult result = new DescribeConfigsResponseData.DescribeConfigsResult();
        result.setErrorCode(error.code());
        result.setConfigs(createConfigEntries(configs));
        results.add(result);
        return new DescribeConfigsResponse(new DescribeConfigsResponseData().setResults(results));
    }

    public List<DescribeConfigsResponseData.DescribeConfigsResourceResult> createConfigEntries(Map<String, String> configs) {
        List<DescribeConfigsResponseData.DescribeConfigsResourceResult> results = new ArrayList<>();

        configs.entrySet().forEach(entry -> {
            results.add(new DescribeConfigsResponseData.DescribeConfigsResourceResult().setName(entry.getKey()).setValue(entry.getValue()));
        });

        return results;
    }
}
