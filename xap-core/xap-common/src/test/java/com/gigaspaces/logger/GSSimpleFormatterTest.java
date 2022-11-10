/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.logger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.Assert.*;

public class GSSimpleFormatterTest {

    public static final String CEF_PATTERN = "{0,date,yyyy-MM-dd HH:mm:ss} {11} CEF:0|gigaspaces|{12}|{13}|0|Legacy Message|{14}|{15}";

    private GSSimpleFormatter gsSimpleFormatter;

    @Before
    public void setUp() throws Exception {
        gsSimpleFormatter = new GSSimpleFormatter(CEF_PATTERN);
    }

    @Test
    public void formatPipeCEF() {
        LogRecord logRecord = new LogRecord(Level.INFO, "detected a | in message");
        String log = gsSimpleFormatter.format(logRecord);
        Assert.assertTrue("Unescape symbol find!", log.contains("detected a \\| in message"));
    }


    @Test
    public void formatMultiLineCEF() {
        LogRecord logRecord = new LogRecord(Level.INFO, "Detected a threat.\n No action needed.");
        String log = gsSimpleFormatter.format(logRecord);
        Assert.assertTrue("Unescape symbol find!", log.contains("Detected a threat.\\n No action needed."));
    }

}