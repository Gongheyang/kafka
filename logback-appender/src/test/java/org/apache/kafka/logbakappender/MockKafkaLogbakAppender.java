/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.logbakappender;

import java.util.Properties;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.test.MockSerializer;

import ch.qos.logback.classic.spi.ILoggingEvent;

public class MockKafkaLogbakAppender extends KafkaLogbakAppender {
    private MockProducer<byte[], byte[]> mockProducer =
            new MockProducer<byte[], byte[]>(false, new MockSerializer(), new MockSerializer());

    @Override
    protected Producer<byte[], byte[]> getKafkaProducer(Properties props) {
        return mockProducer;
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (super.getProducer() == null) {
            start();
        }
        super.append(event);
    }

    protected java.util.List<ProducerRecord<byte[], byte[]>> getHistory() {
        return mockProducer.history();
    }
}
