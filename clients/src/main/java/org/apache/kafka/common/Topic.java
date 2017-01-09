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
package org.apache.kafka.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Topic {

    private static final String INVALID_CHARS = "[^a-zA-Z0-9._\\-]";
    private static final int MAX_NAME_LENGTH = 249;

    public static void validate(String topic) {
        if (isEmpty(topic))
            throw new org.apache.kafka.common.errors.InvalidTopicException("topic name is illegal, can't be empty");
        else if (containsOnlyPeriods(topic))
            throw new org.apache.kafka.common.errors.InvalidTopicException("topic name cannot be \".\" or \"..\"");
        else if (exceedsMaxLength(topic))
            throw new org.apache.kafka.common.errors.InvalidTopicException("topic name is illegal, can't be longer than " + MAX_NAME_LENGTH + " characters");
        else if (containsInvalidCharacters(topic)) throw new org.apache.kafka.common.errors.InvalidTopicException("topic name " + topic + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'");
    }

    public static boolean isEmpty(String topic) {
        return topic.length() <= 0;
    }

    public static boolean containsOnlyPeriods(String topic) {
        return topic.equals(".") || topic.equals("..");
    }

    public static boolean exceedsMaxLength(String topic) {
        return topic.length() > MAX_NAME_LENGTH;
    }

    /**
     * Valid characters for Kafka topics are the ASCII alphanumerics, '.', '_', and '-'
     */
    public static boolean containsInvalidCharacters(String topic) {
        Pattern pattern = Pattern.compile(INVALID_CHARS);
        Matcher matcher = pattern.matcher(topic);
        return matcher.find();
    }

}
