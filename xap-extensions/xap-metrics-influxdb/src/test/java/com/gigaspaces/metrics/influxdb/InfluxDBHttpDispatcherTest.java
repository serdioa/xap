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
