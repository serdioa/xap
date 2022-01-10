package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.metrics.*;
import com.j_spaces.kernel.SystemProperties;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;


public class HsqlDbReporterTest {

    @Test
    public void testDisableMetrics() {
        System.setProperty(SystemProperties.RECORDING_OF_ALL_METRICS_TO_HSQLDB_ENABLED, "false");

        SharedJdbcConnectionWrapper connectionWrapper = Mockito.mock(SharedJdbcConnectionWrapper.class);
        HsqlDBReporterFactory factory = Mockito.mock(HsqlDBReporterFactory.class);
        Connection jdbcConnection = Mockito.mock(Connection.class);
        SystemMetricsManager systemMetricsManager = Mockito.mock(SystemMetricsManager.class);

        when(factory.getDbTypeString()).thenReturn("VARCHAR(300)");
        when(factory.getDriverClassName()).thenReturn("org.hsqldb.jdbc.JDBCDriver");
        doReturn(jdbcConnection).when(connectionWrapper).getOrCreateConnection();

        HsqlDbReporter reporter = new HsqlDbReporter(factory, connectionWrapper);

        MetricRegistry registry = new MetricRegistry("foo");
        MetricTags tags = createTags();
        LongCounter counter1 = new LongCounter();
        registry.register(PredefinedSystemMetrics.PROCESS_CPU_USED_PERCENT.getMetricName(), tags, counter1);
        MetricRegistrySnapshot snapshot = registry.snapshot(10l);
        reporter.report(Collections.singletonList(snapshot));

        verify(connectionWrapper, times(1)).getOrCreateConnection();
    }

    private MetricTags createTags() {
        Map<String, Object> tags = new HashMap<String, Object>();
        tags.put("foo", "bar");
        return new MetricTags(tags);
    }

}
