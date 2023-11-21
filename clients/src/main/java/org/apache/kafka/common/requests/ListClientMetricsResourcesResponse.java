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
package org.apache.kafka.common.requests;

import org.apache.kafka.clients.admin.ClientMetricsResourceListing;
import org.apache.kafka.common.message.ListClientMetricsResourcesResponseData.ClientMetricsResource;
import org.apache.kafka.common.message.ListClientMetricsResourcesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class ListClientMetricsResourcesResponse extends AbstractResponse {
    private final ListClientMetricsResourcesResponseData data;

    public ListClientMetricsResourcesResponse(ListClientMetricsResourcesResponseData data) {
        super(ApiKeys.LIST_CLIENT_METRICS_RESOURCES);
        this.data = data;
    }

    public ListClientMetricsResourcesResponseData data() {
        return data;
    }

    public ApiError error() {
        return new ApiError(Errors.forCode(data.errorCode()));
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    public static ListClientMetricsResourcesResponse parse(ByteBuffer buffer, short version) {
        return new ListClientMetricsResourcesResponse(new ListClientMetricsResourcesResponseData(
            new ByteBufferAccessor(buffer), version));
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public Collection<ClientMetricsResourceListing> clientMetricsResources() {
        ArrayList<ClientMetricsResourceListing> resources = new ArrayList<>(data.clientMetricsResources().size());
        for (ClientMetricsResource entry : data.clientMetricsResources()) {
            ClientMetricsResourceListing cmrl = new ClientMetricsResourceListing(entry.name());
            resources.add(cmrl);
        }
        return resources;
    }
}
