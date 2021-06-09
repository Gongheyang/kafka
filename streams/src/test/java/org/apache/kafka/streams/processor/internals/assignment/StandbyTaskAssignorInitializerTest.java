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
package org.apache.kafka.streams.processor.internals.assignment;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;

public class StandbyTaskAssignorInitializerTest {

    private StandbyTaskAssignorInitializer standbyTaskAssignorInitializer;

    @Before
    public void setup() {
        standbyTaskAssignorInitializer = new StandbyTaskAssignorInitializer();
    }

    @Test
    public void shouldReturnNoopStandbyTaskAssignorWhenNumOfStandbyReplicasConfigIsZero() {
        final StandbyTaskAssignor standbyTaskAssignor = standbyTaskAssignorInitializer.initStandbyTaskAssignor(newAssignmentConfigs(0));
        assertTrue(standbyTaskAssignor instanceof NoopStandbyTaskAssignor);
    }

    @Test
    public void shouldReturnClientTagAwareStandbyTaskAssignorWhenRackAwareAssignmentTagsIsSet() {
        final StandbyTaskAssignor standbyTaskAssignor = standbyTaskAssignorInitializer.initStandbyTaskAssignor(newAssignmentConfigs(1, singletonList("az")));
        assertTrue(standbyTaskAssignor instanceof ClientTagAwareStandbyTaskAssignor);
    }

    @Test
    public void shouldReturnDefaultStandbyTaskAssignorWhenRackAwareAssignmentTagsIsEmpty() {
        final StandbyTaskAssignor standbyTaskAssignor = standbyTaskAssignorInitializer.initStandbyTaskAssignor(newAssignmentConfigs(1, Collections.emptyList()));
        assertTrue(standbyTaskAssignor instanceof DefaultStandbyTaskAssignor);
    }

    private static AssignorConfiguration.AssignmentConfigs newAssignmentConfigs(final int numStandbyReplicas) {
        return newAssignmentConfigs(numStandbyReplicas, Collections.emptyList());
    }

    private static AssignorConfiguration.AssignmentConfigs newAssignmentConfigs(final int numStandbyReplicas,
                                                                                final List<String> rackAwareAssignmentTags) {
        return new AssignorConfiguration.AssignmentConfigs(0L,
                                                           1,
                                                           numStandbyReplicas,
                                                           60000L,
                                                           rackAwareAssignmentTags);
    }
}