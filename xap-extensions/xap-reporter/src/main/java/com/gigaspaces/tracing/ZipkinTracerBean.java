package com.gigaspaces.tracing;

import brave.Tracing;
import brave.opentracing.BraveTracer;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;
import io.opentracing.util.GlobalTracer;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.okhttp3.OkHttpSender;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

public class ZipkinTracerBean {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private AsyncReporter<Span> reporter;
    private Tracing tracing;
    private BraveTracer tracer;
    private boolean startActive = false;
    private String serviceName;
    private String zipkinUrl = "http://zipkin.service.consul:9411";

    public ConsulClient getClient(String agentHost) {
        try {

            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
                    builder.build());
            CloseableHttpClient customHttpClient = HttpClients.custom().setSSLSocketFactory(
                    sslsf).build();
            ConsulRawClient rawClient = new ConsulRawClient(agentHost, customHttpClient);


            return new ConsulClient(rawClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ZipkinTracerBean() {
        if (GlobalTracer.isRegistered()) throw new IllegalArgumentException("GlobalTracer already exists");
    }

    public ZipkinTracerBean(String serviceName) {
        this();
        this.serviceName = serviceName;
    }

    public ZipkinTracerBean setStartActive(boolean startActive) {
        this.startActive = startActive;
        return this;
    }

    public ZipkinTracerBean setZipkinUrl(String zipkinUrl) {
        this.zipkinUrl = zipkinUrl;
        return this;
    }

    @PostConstruct
    public void start() {
        logger.info("Starting " + (startActive ? "active" : "inactive") + " with service name [" + serviceName + "]");
        logger.info("Connecting to Zipkin at " + zipkinUrl);

        OkHttpSender sender = OkHttpSender.create(
                zipkinUrl + "/api/v2/spans");
        reporter = AsyncReporter.builder(sender).build();
        tracing = Tracing.newBuilder()
                .localServiceName(serviceName)
                .spanReporter(reporter)
                .build();
        if (!startActive) {
            tracing.setNoop(true);
        }

        tracer = BraveTracer.create(tracing);
        GlobalTracer.registerIfAbsent(tracer);
    }


    @PreDestroy
    public void destroy() throws Exception {

        if (tracer != null) {
            tracer.close();
        }

        if (reporter != null) {
            reporter.close();
        }

    }
}
