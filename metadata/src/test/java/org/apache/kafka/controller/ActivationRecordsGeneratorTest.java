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

package org.apache.kafka.controller;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * This class is for testing the log message or exception produced by ActivationRecordsGenerator. For tests that
 * verify the semantics of the returned records, see QuorumControllerTest.
 */
public class ActivationRecordsGeneratorTest {

    @Test
    public void testActivationMessageForEmptyLog() {
        ControllerResult<Void> result;
        ConfigurationControlManager configurationControl = Mockito.mock(ConfigurationControlManager.class);
        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. The metadata log appears to be empty. " +
                "Appending 1 bootstrap record(s) at metadata.version 3.0-IV1 from bootstrap source 'test'.", logMsg),
            -1L,
            BootstrapMetadata.fromVersion(MetadataVersion.MINIMUM_BOOTSTRAP_VERSION, "test"),
            MetadataVersion.MINIMUM_KRAFT_VERSION,
            false,
            configurationControl
        );
        assertTrue(result.isAtomic());
        assertEquals(1, result.records().size());

        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. The metadata log appears to be empty. " +
                "Appending 1 bootstrap record(s) at metadata.version 3.4-IV0 from bootstrap " +
                "source 'test'.", logMsg),
            -1L,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_4_IV0, "test"),
            MetadataVersion.IBP_3_4_IV0,
            false,
            configurationControl
        );
        assertTrue(result.isAtomic());
        assertEquals(1, result.records().size());

        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. The metadata log appears to be empty. " +
                "Appending 1 bootstrap record(s) in metadata transaction at metadata.version 3.6-IV1 from bootstrap " +
                "source 'test'.", logMsg),
            -1L,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV1, "test"),
            MetadataVersion.IBP_3_6_IV1,
            false,
            configurationControl
        );
        assertFalse(result.isAtomic());
        assertEquals(3, result.records().size());

        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Aborting partial bootstrap records " +
                "transaction at offset 0. Re-appending 1 bootstrap record(s) in new metadata transaction at " +
                "metadata.version 3.6-IV1 from bootstrap source 'test'.", logMsg),
            0L,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV1, "test"),
            MetadataVersion.IBP_3_6_IV1,
            false,
            configurationControl
        );
        assertFalse(result.isAtomic());
        assertEquals(4, result.records().size());

        verify(configurationControl, never()).maybeResetMinIsrConfig(any());

        Mockito.doAnswer(invocation -> {
            List<ApiMessageAndVersion> output = invocation.getArgument(0);
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
            output.add(new ApiMessageAndVersion(new ConfigRecord().
                setResourceType(configResource.type().id()).
                setResourceName(configResource.name()).
                setName(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).
                setValue("2"), (short) 0));
            return null;
        }).when(configurationControl).maybeResetMinIsrConfig(any());

        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Aborting partial bootstrap records " +
                "transaction at offset 0. Re-appending 1 bootstrap record(s) in new metadata transaction at " +
                "metadata.version 4.0-IV1 from bootstrap source 'test'.", logMsg),
            0L,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_4_0_IV1, "test"),
            MetadataVersion.IBP_4_0_IV1,
            true,
            configurationControl
        );
        assertFalse(result.isAtomic());
        assertEquals(5, result.records().size());
    }
}
