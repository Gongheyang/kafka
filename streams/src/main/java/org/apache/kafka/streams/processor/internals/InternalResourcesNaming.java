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
package org.apache.kafka.streams.processor.internals;

public final class InternalResourcesNaming {

    private String repartitionTopic;
    private String changelogTopic;
    private String stateStore;

    private InternalResourcesNaming() {
    }

    public static InternalResourcesNaming build() {
        return new InternalResourcesNaming();
    }

    public InternalResourcesNaming withRepartitionTopic(final String repartitionTopic) {
        this.repartitionTopic = repartitionTopic;
        return this;
    }

    public InternalResourcesNaming withChangelogTopic(final String changelogTopic) {
        this.changelogTopic = changelogTopic;
        return this;
    }

    public InternalResourcesNaming withStateStore(final String stateStore) {
        this.stateStore = stateStore;
        return this;
    }

    public String repartitionTopic() {
        return repartitionTopic;
    }

    public String changelogTopic() {
        return changelogTopic;
    }

    public String stateStore() {
        return stateStore;
    }
}
