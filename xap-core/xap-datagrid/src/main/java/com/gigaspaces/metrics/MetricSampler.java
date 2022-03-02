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

package com.gigaspaces.metrics;

import com.gigaspaces.internal.utils.concurrent.GSThreadFactory;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.metrics.internal.GaugeContextProvider;
import com.gigaspaces.metrics.internal.InternalGauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class MetricSampler implements Closeable {

    private final Object lock;
    private final Logger logger;
    private final MetricRegistry registry;
    private final Set<GaugeContextProvider> contextProviders;
    private final ScheduledExecutorService executor;
    private final long samplingRate;
    private final int batchSize;

    private final Collection<MetricReporter> reporters;
    private final String name;
    private ScheduledFuture<?> future;

    public MetricSampler(MetricSamplerConfig config, Collection<MetricReporter> reporters) {
        this.lock = new Object();
        this.name = config.getName();
        this.logger = LoggerFactory.getLogger(Constants.LOGGER_METRICS_SAMPLER + '.' + name);
        this.registry = new MetricRegistry(name);
        this.contextProviders = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.samplingRate = config.getSamplingRate();
        this.batchSize = config.getBatchSize();
        this.reporters = reporters;
        this.executor = Executors.newSingleThreadScheduledExecutor(new GSThreadFactory("metrics-sampler-" + config.getName(), true));
        if (logger.isDebugEnabled())
            logger.debug("Metric sampler created [sampleRate=" + samplingRate + "ms, batchSize=" + batchSize + "]");
    }

    Collection<MetricReporter> getReporters() {
        return reporters;
    }

    private boolean shouldBeActive() {
        return !reporters.isEmpty() && !registry.isEmpty() && samplingRate > 0;
    }

    public boolean isActive() {
        return future != null;
    }

    /**
     * Stops the reporter and shuts down its thread of execution.
     */
    @Override
    public void close() {
        if (logger.isDebugEnabled())
            logger.debug("Closing metric sampler");

        /* Uses the shutdown pattern from http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html */

        // Disable new tasks from being submitted
        executor.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                executor.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    logger.warn("ScheduledExecutorService did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }

        // Close the reporters:
        for (MetricReporter reporter : reporters)
            reporter.close();
    }

    public void register(String name, MetricTags tags, Metric metric) {
        registry.register(name, tags, metric);
        if (metric instanceof InternalGauge) {
            GaugeContextProvider contextProvider = ((InternalGauge) metric).getContextProvider();
            if (contextProvider != null)
                contextProviders.add(contextProvider);
        }

        // activate if needed:
        if (!isActive() && shouldBeActive()) {
            synchronized (lock) {
                if (!isActive() && shouldBeActive()) {
                    future = executor.scheduleAtFixedRate(new Sampler(), 0, samplingRate, TimeUnit.MILLISECONDS);
                    if (logger.isDebugEnabled())
                        logger.debug("Started metric sampler [sample-rate=" + samplingRate + "ms]");
                }
            }
        }
    }

    public void remove(String metricName, MetricTags tags) {
        registry.remove(metricName, tags);

        // deactivate if needed:
        if (isActive() && !shouldBeActive()) {
            synchronized (lock) {
                if (isActive() && !shouldBeActive()) {
                    future.cancel(true);
                    future = null;
                }
            }
        }
    }

    public void removeByPrefix(String prefix, MetricTags tags) {
        registry.removeByPrefix(prefix, tags);

        // deactivate if needed:
        if (isActive() && !shouldBeActive()) {
            synchronized (lock) {
                if (isActive() && !shouldBeActive()) {
                    future.cancel(true);
                    future = null;
                }
            }
        }
    }

    public Map<String, Object> getSnapshotsByPrefixAndMatchingTags(String prefix, MetricTags tags) {
        return registry.getSnapshotByPrefixAndMatchingTags(prefix, tags);
    }

    public Map<String,Object> getSnapshotsByPrefix(String prefix) {
        return registry.getSnapshotsByPrefix(prefix);
    }

    public MetricRegistry getRegistry() {
        return registry;
    }

    public String getName() {
        return name;
    }

    private class Sampler implements Runnable {
        private final List<MetricRegistrySnapshot> buffer;

        private Sampler() {
            this.buffer = new ArrayList<>(batchSize);
        }

        @Override
        public void run() {
            if (logger.isTraceEnabled())
                logger.trace("Sample started");

            for (GaugeContextProvider contextProvider : contextProviders)
                contextProvider.reset();

            final long sampleTime = System.currentTimeMillis();
            final MetricRegistrySnapshot snapshot = registry.snapshot(sampleTime);
            if (logger.isTraceEnabled()) {
                final long duration = System.currentTimeMillis() - sampleTime;
                logger.trace("Snapshot completed [sampleTime=" + sampleTime +
                        ", duration=" + duration + "ms" +
                        ", groups=" + snapshot.getGroups().size() +
                        ", metrics=" + snapshot.getTotalMetrics() +
                        "]");
            }
            buffer.add(snapshot);

            if (buffer.size() == batchSize) {
                for (MetricReporter reporter : reporters) {
                    long startTime = 0;
                    if (logger.isTraceEnabled()) {
                        startTime = System.currentTimeMillis();
                        logger.trace("Starting buffer report via " + reporter.toString());
                    }
                    reporter.report(buffer);
                    if (logger.isTraceEnabled()) {
                        long duration = System.currentTimeMillis() - startTime;
                        logger.trace("Finished buffer report via " + reporter.toString() + "[duration=" + duration + "ms]");
                    }
                }
                buffer.clear();
            }

            if (logger.isTraceEnabled()) {
                final long duration = System.currentTimeMillis() - sampleTime;
                logger.trace("Sample completed [duration=" + duration + "ms]");
            }
        }
    }

    @Override
    public String toString() {
        return "MetricSampler{" +
                "name='" + name + '\'' +
                '}';
    }
}
