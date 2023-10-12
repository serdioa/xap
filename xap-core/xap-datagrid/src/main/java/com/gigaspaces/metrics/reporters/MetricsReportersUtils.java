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
package com.gigaspaces.metrics.reporters;

import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.metrics.DbReporterFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author evgeny
 * @since 16.2.0
 */
public class MetricsReportersUtils {

    public static URL toInfluxDbUrl(String operationName, String encodedQuery, DbReporterFactory factory) {
        return toInfluxDbUrl(operationName, encodedQuery, factory, Collections.emptyMap());
    }

    public static URL toInfluxDbUrl(String operationName, String encodedQuery, DbReporterFactory factory, Map<String,String> parameters) {
        // See https://influxdb.com/docs/v0.9/guides/writing_data.html
        // "http://localhost:8086/write?db=db1");
        try {
            if (!StringUtils.hasLength(factory.getHost()))
                throw new IllegalArgumentException("Mandatory property not provided - host");
            if (!StringUtils.hasLength(factory.getDatabase()))
                throw new IllegalArgumentException("Mandatory property not provided - database");
            String suffix = "/" + operationName + "?db=" + factory.getDatabase();
            suffix = append(suffix, "rp", factory.getRetentionPolicy());
            suffix = append(suffix, "u", factory.getUsername());
            suffix = append(suffix, "p", factory.getPassword());
            suffix = append(suffix, "precision", toString(factory.getTimePrecision()));
            suffix = append(suffix, "consistency", factory.getConsistency());

            if (encodedQuery != null) {
                //Returns epoch timestamps
                suffix = append(suffix, "epoch", "ms");
                suffix = append(suffix, "q", encodedQuery);
            }

            for( Map.Entry<String, String> parameter : parameters.entrySet() ){
                suffix = append(suffix, parameter.getKey(), parameter.getValue());
            }

            return new URL(factory.getProtocol(), factory.getHost(), factory.getPort(), suffix);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Failed to create InfluxDB HTTP url", e);
        }
    }

    private static String append(String prefix, String name, String value) {
        return StringUtils.hasLength(value) ? prefix + '&' + name + '=' + value : prefix;
    }

    private static String toString(TimeUnit timeUnit) {
        // https://influxdb.com/docs/v0.9/write_protocols/write_syntax.html#http
        if (timeUnit == null)
            return null;
        if (timeUnit == TimeUnit.NANOSECONDS)
            return "n";
        if (timeUnit == TimeUnit.MICROSECONDS)
            return "u";
        if (timeUnit == TimeUnit.MILLISECONDS)
            return "ms";
        if (timeUnit == TimeUnit.SECONDS)
            return "s";
        if (timeUnit == TimeUnit.MINUTES)
            return "m";
        if (timeUnit == TimeUnit.HOURS)
            return "h";
        throw new IllegalArgumentException("Unsupported time precision: " + timeUnit);
    }
}