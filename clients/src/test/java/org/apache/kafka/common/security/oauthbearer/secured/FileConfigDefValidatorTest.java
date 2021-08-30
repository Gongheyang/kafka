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

package org.apache.kafka.common.security.oauthbearer.secured;

import java.io.File;
import java.io.IOException;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

public class FileConfigDefValidatorTest extends ConfigDefValidatorTest {

    @Override
    public Validator createValidator() {
        return new FileConfigDefValidator();
    }

    @Test
    public void test() throws IOException {
        File file = TestUtils.tempFile("some contents!");
        ensureValid(file.getAbsolutePath());
    }

    @Test
    public void testWithSuperfluousWhitespace() throws IOException {
        File file = TestUtils.tempFile();
        ensureValid(String.format("  %s  ", file.getAbsolutePath()));
    }

    @Test
    public void testFileDoesNotExist() {
        assertThrowsWithMessage(ConfigException.class, () -> ensureValid("/tmp/not/a/real/file.txt"), "that doesn't exist");
    }

    @Test
    public void testFileUnreadable() throws IOException {
        File file = TestUtils.tempFile();

        if (!file.setReadable(false))
            throw new IllegalStateException(String.format("Can't test file permissions as test couldn't programmatically make temp file %s un-readable", file.getAbsolutePath()));

        assertThrowsWithMessage(ConfigException.class, () -> ensureValid(file.getAbsolutePath()), "that doesn't have read permission");
    }

}
