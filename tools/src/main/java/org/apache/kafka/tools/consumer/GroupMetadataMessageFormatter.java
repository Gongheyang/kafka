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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordSerde;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValueJsonConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class GroupMetadataMessageFormatter implements MessageFormatter {
    private static final String VERSION = "version";
    private static final String DATA = "data";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    static final String UNKNOWN = "unknown";

    private final GroupCoordinatorRecordSerde serde = new GroupCoordinatorRecordSerde();

    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
        ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
        JsonNode keyNode = NullNode.getInstance();
        JsonNode valueNode = NullNode.getInstance();
        try {
            CoordinatorRecord record;
            if (consumerRecord.value() != null) {
                record = serde.deserialize(ByteBuffer.wrap(consumerRecord.key()), ByteBuffer.wrap(consumerRecord.value()));
                valueNode = transferValueMessageToJsonNode(record.value().message(), record.value().version());
            } else {
                record = serde.deserialize(ByteBuffer.wrap(consumerRecord.key()), null);
            }
            keyNode = transferKeyMessageToJsonNode(record.key().message(), record.key().version());
        } catch (CoordinatorLoader.UnknownRecordTypeException e) {
            keyNode = new TextNode(UNKNOWN);
            valueNode = new TextNode(UNKNOWN);
        }

        if (keyNode instanceof NullNode) {
            return;
        } else {
            json.putObject(KEY)
                .put(VERSION, ByteBuffer.wrap(consumerRecord.key()).getShort())
                .set(DATA, keyNode);
        }

        if (valueNode instanceof NullNode) {
            json.set(VALUE, valueNode);
        } else {
            json.putObject(VALUE)
                .put(VERSION, ByteBuffer.wrap(consumerRecord.value()).getShort())
                .set(DATA, valueNode);
        }

        try {
            output.write(json.toString().getBytes(UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"CyclomaticComplexity"})
    private JsonNode transferKeyMessageToJsonNode(ApiMessage message, short version) {
        if (message instanceof OffsetCommitKey) {
            return NullNode.getInstance();
        } else if (message instanceof GroupMetadataKey) {
            return GroupMetadataKeyJsonConverter.write((GroupMetadataKey) message, version);
        } else if (message instanceof ConsumerGroupMetadataKey) {
            return ConsumerGroupMetadataKeyJsonConverter.write((ConsumerGroupMetadataKey) message, version);
        } else if (message instanceof ConsumerGroupPartitionMetadataKey) {
            return ConsumerGroupPartitionMetadataKeyJsonConverter.write((ConsumerGroupPartitionMetadataKey) message, version);
        } else if (message instanceof ConsumerGroupMemberMetadataKey) {
            return ConsumerGroupMemberMetadataKeyJsonConverter.write((ConsumerGroupMemberMetadataKey) message, version);
        } else if (message instanceof ConsumerGroupTargetAssignmentMetadataKey) {
            return ConsumerGroupTargetAssignmentMetadataKeyJsonConverter.write((ConsumerGroupTargetAssignmentMetadataKey) message, version);
        } else if (message instanceof ConsumerGroupTargetAssignmentMemberKey) {
            return ConsumerGroupTargetAssignmentMemberKeyJsonConverter.write((ConsumerGroupTargetAssignmentMemberKey) message, version);
        } else if (message instanceof ConsumerGroupCurrentMemberAssignmentKey) {
            return ConsumerGroupCurrentMemberAssignmentKeyJsonConverter.write((ConsumerGroupCurrentMemberAssignmentKey) message, version);
        } else if (message instanceof ShareGroupPartitionMetadataKey) {
            return ShareGroupPartitionMetadataKeyJsonConverter.write((ShareGroupPartitionMetadataKey) message, version);
        } else if (message instanceof ShareGroupMemberMetadataKey) {
            return ShareGroupMemberMetadataKeyJsonConverter.write((ShareGroupMemberMetadataKey) message, version);
        } else if (message instanceof ShareGroupMetadataKey) {
            return ShareGroupMetadataKeyJsonConverter.write((ShareGroupMetadataKey) message, version);
        } else if (message instanceof ShareGroupTargetAssignmentMetadataKey) {
            return ShareGroupTargetAssignmentMetadataKeyJsonConverter.write((ShareGroupTargetAssignmentMetadataKey) message, version);
        } else if (message instanceof ShareGroupTargetAssignmentMemberKey) {
            return ShareGroupTargetAssignmentMemberKeyJsonConverter.write((ShareGroupTargetAssignmentMemberKey) message, version);
        } else if (message instanceof ShareGroupCurrentMemberAssignmentKey) {
            return ShareGroupCurrentMemberAssignmentKeyJsonConverter.write((ShareGroupCurrentMemberAssignmentKey) message, version);
        } else if (message instanceof ShareGroupStatePartitionMetadataKey) {
            return ShareGroupStatePartitionMetadataKeyJsonConverter.write((ShareGroupStatePartitionMetadataKey) message, version);
        } else if (message instanceof ConsumerGroupRegularExpressionKey) {
            return ConsumerGroupRegularExpressionKeyJsonConverter.write((ConsumerGroupRegularExpressionKey) message, version);
        } else {
            return new TextNode(UNKNOWN);
        }
    }

    private JsonNode transferValueMessageToJsonNode(ApiMessage message, short version) {
        if (message instanceof GroupMetadataValue) {
            return GroupMetadataValueJsonConverter.write((GroupMetadataValue) message, version);
        } else if (message instanceof ConsumerGroupMetadataValue) {
            return ConsumerGroupMetadataValueJsonConverter.write((ConsumerGroupMetadataValue) message, version);
        } else if (message instanceof ConsumerGroupPartitionMetadataValue) {
            return ConsumerGroupPartitionMetadataValueJsonConverter.write((ConsumerGroupPartitionMetadataValue) message, version);
        } else if (message instanceof ConsumerGroupMemberMetadataValue) {
            return ConsumerGroupMemberMetadataValueJsonConverter.write((ConsumerGroupMemberMetadataValue) message, version);
        } else if (message instanceof  ConsumerGroupTargetAssignmentMetadataValue) {
            return ConsumerGroupTargetAssignmentMetadataValueJsonConverter.write((ConsumerGroupTargetAssignmentMetadataValue) message, version);
        } else if (message instanceof ConsumerGroupTargetAssignmentMemberValue) {
            return ConsumerGroupTargetAssignmentMemberValueJsonConverter.write((ConsumerGroupTargetAssignmentMemberValue) message, version);
        } else if (message instanceof ConsumerGroupCurrentMemberAssignmentValue) {
            return ConsumerGroupCurrentMemberAssignmentValueJsonConverter.write((ConsumerGroupCurrentMemberAssignmentValue) message, version);
        } else if (message instanceof ShareGroupPartitionMetadataValue) {
            return ShareGroupPartitionMetadataValueJsonConverter.write((ShareGroupPartitionMetadataValue) message, version);
        } else if (message instanceof ShareGroupMemberMetadataValue) {
            return ShareGroupMemberMetadataValueJsonConverter.write((ShareGroupMemberMetadataValue) message, version);
        } else if (message instanceof ShareGroupMetadataValue) {
            return ShareGroupMetadataValueJsonConverter.write((ShareGroupMetadataValue) message, version);
        } else if (message instanceof ShareGroupTargetAssignmentMetadataValue) {
            return ShareGroupTargetAssignmentMetadataValueJsonConverter.write((ShareGroupTargetAssignmentMetadataValue) message, version);
        } else if (message instanceof ShareGroupTargetAssignmentMemberValue) {
            return ShareGroupTargetAssignmentMemberValueJsonConverter.write((ShareGroupTargetAssignmentMemberValue) message, version);
        } else if (message instanceof ShareGroupCurrentMemberAssignmentValue) {
            return ShareGroupCurrentMemberAssignmentValueJsonConverter.write((ShareGroupCurrentMemberAssignmentValue) message, version);
        } else if (message instanceof ShareGroupStatePartitionMetadataValue) {
            return ShareGroupStatePartitionMetadataValueJsonConverter.write((ShareGroupStatePartitionMetadataValue) message, version);
        } else if (message instanceof ConsumerGroupRegularExpressionValue) {
            return ConsumerGroupRegularExpressionValueJsonConverter.write((ConsumerGroupRegularExpressionValue) message, version);
        } else {
            return new TextNode(UNKNOWN);
        }
    }
}
