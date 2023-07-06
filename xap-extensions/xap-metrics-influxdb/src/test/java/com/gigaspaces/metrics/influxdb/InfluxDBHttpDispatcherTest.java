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
package com.gigaspaces.metrics.influxdb;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class InfluxDBHttpDispatcherTest {

    @Test
    public void defaultProtocolTest() {
        // Test default
        Properties properties = new Properties();
        properties.setProperty("host", "foo");
        properties.setProperty("database", "mydb");
        InfluxDBReporterFactory factory = new InfluxDBReporterFactory();
        factory.load(properties);
        InfluxDBHttpDispatcher dispatcher = new InfluxDBHttpDispatcher(factory);
        Assert.assertEquals(dispatcher.getUrl().getProtocol(), "http");

        // https
        properties.setProperty("protocol", "https");
        factory = new InfluxDBReporterFactory();
        factory.load(properties);
        dispatcher = new InfluxDBHttpDispatcher(factory);
        Assert.assertEquals(dispatcher.getUrl().getProtocol(), "https");
    }

    @Test
    public void httpsProtocolTest() {
        // https
        // Test default
        Properties properties = new Properties();
        properties.setProperty("host", "foo");
        properties.setProperty("database", "mydb");
        properties.setProperty("protocol", "https");
        InfluxDBReporterFactory factory = new InfluxDBReporterFactory();
        factory.load(properties);
        InfluxDBHttpDispatcher dispatcher = new InfluxDBHttpDispatcher(factory);
        Assert.assertEquals(dispatcher.getUrl().getProtocol(), "https");
    }

}
