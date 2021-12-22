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

import com.gigaspaces.metrics.HttpUtils;
import com.gigaspaces.metrics.reporters.MetricsReportersUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

/**
 * @author Niv Ingberg
 * @since 10.2.1
 */
public class InfluxDBHttpDispatcher extends InfluxDBDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBHttpDispatcher.class.getName());
    private static final String CONTENT_TYPE = System.getProperty("com.gigaspaces.metrics.influxdb.http.content_type", "text/plain");
    private static final int TIMEOUT = Integer.getInteger("com.gigaspaces.metrics.influxdb.http.timeout", 30000);

    private final URL url;

    public InfluxDBHttpDispatcher(InfluxDBReporterFactory factory) {
        this.url = toUrl("write", factory);
        logger.debug("InfluxDBHttpDispatcher created [url=" + url + "]");
    }

    public URL getUrl() {
        return url;
    }

    @Override
    protected void doSend(String content) throws IOException {
        int httpCode = HttpUtils.post(url, content, CONTENT_TYPE, TIMEOUT);
        if (httpCode != 204)
            throw new IOException("Failed to post [HTTP Code=" + httpCode + ", url=" + url.toString() + "]");
    }

    private static URL toUrl(String operationName, InfluxDBReporterFactory factory) {
        return MetricsReportersUtils.toInfluxDbUrl(operationName, null, factory);
    }
}