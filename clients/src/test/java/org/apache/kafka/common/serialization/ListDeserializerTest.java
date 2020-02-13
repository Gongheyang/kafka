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
package org.apache.kafka.common.serialization;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ListDeserializerTest {
    private final ListDeserializer<?> listDeserializer = new ListDeserializer<>();
    private final Map<String, Object> props = new HashMap<>();
    private final String nonExistingClass = "non.existing.class";
    private static class FakeObject {
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsWithClassNames() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class.getName());
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class.getName());
        listDeserializer.configure(props, true);
        final Deserializer<?> inner = listDeserializer.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner);
        assertTrue("Inner deserializer type should be StringDeserializer", inner instanceof StringDeserializer);
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsWithClassNames() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, ArrayList.class.getName());
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.IntegerSerde.class.getName());
        listDeserializer.configure(props, false);
        final Deserializer<?> inner = listDeserializer.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner);
        assertTrue("Inner deserializer type should be IntegerDeserializer", inner instanceof IntegerDeserializer);
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsWithClassObjects() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        listDeserializer.configure(props, true);
        final Deserializer<?> inner = listDeserializer.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner);
        assertTrue("Inner deserializer type should be StringDeserializer", inner instanceof StringDeserializer);
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsWithClassObjects() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        listDeserializer.configure(props, false);
        final Deserializer<?> inner = listDeserializer.innerDeserializer();
        assertNotNull("Inner deserializer should be not null", inner);
        assertTrue("Inner deserializer type should be StringDeserializer", inner instanceof StringDeserializer);
    }



    @Test
    public void testListKeyDeserializerNoArgConstructorsShouldThrowConfigExceptionDueMissingInnerClassProp() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertThat(exception.getMessage(), is("Not able to determine the inner serde class because it was neither passed via the constructor nor set in the config."));
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsShouldThrowConfigExceptionDueMissingTypeClassProp() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertThat(exception.getMessage(), is("Not able to determine the list class because it was neither passed via the constructor nor set in the config."));
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidTypeClass() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, new FakeObject());
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertThat(exception.getMessage(), is("Could not determine the list class instance using \"" + CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS + "\" property."));
    }

    @Test
    public void testListKeyDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidInnerClass() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, new FakeObject());
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertThat(exception.getMessage(), is("Could not determine the inner serde class instance using \"" + CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS + "\" property."));
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidTypeClass() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, new FakeObject());
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listDeserializer.configure(props, false)
        );
        assertThat(exception.getMessage(), is("Could not determine the list class instance using \"" + CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS + "\" property."));
    }

    @Test
    public void testListValueDeserializerNoArgConstructorsShouldThrowKafkaExceptionDueInvalidInnerClass() {
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS, new FakeObject());
        final KafkaException exception = assertThrows(
            KafkaException.class,
            () -> listDeserializer.configure(props, false)
        );
        assertThat(exception.getMessage(), is("Could not determine the inner serde class instance using \"" + CommonClientConfigs.DEFAULT_LIST_VALUE_SERDE_INNER_CLASS + "\" property."));
    }

    @Test
    public void testListDeserializerNoArgConstructorsShouldThrowConfigExceptionDueListClassNotFound() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, nonExistingClass);
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertThat(exception.getMessage(), is("Invalid value " + nonExistingClass + " for configuration " + CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS + ": Deserializer's list class \"" + nonExistingClass + "\" could not be found."));
    }

    @Test
    public void testListDeserializerNoArgConstructorsShouldThrowConfigExceptionDueInnerSerdeClassNotFound() {
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class);
        props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, nonExistingClass);
        final ConfigException exception = assertThrows(
            ConfigException.class,
            () -> listDeserializer.configure(props, true)
        );
        assertThat(exception.getMessage(), is("Invalid value " + nonExistingClass + " for configuration " + CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS + ": Deserializer's inner serde class \"" + nonExistingClass + "\" could not be found."));
    }

}
