/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cdc.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneSpiContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;

import static org.apache.ignite.internal.IgnitionEx.initializeDefaultMBeanServer;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/** CDC kafka to ignite metrics. */
public class KafkaToIgniteMetrics {
    /** Count of events received name. */
    public static final String EVTS_RCVD_CNT = "EventsReceivedCount";

    /** Count of events received description. */
    public static final String EVTS_RCVD_CNT_DESC = "Count of events received from Kafka";

    /** Timestamp of last received event name. */
    public static final String LAST_EVT_RCVD_TIME = "LastEventReceivedTime";

    /** Timestamp of last received event description. */
    public static final String LAST_EVT_RCVD_TIME_DESC = "Timestamp of last received event from Kafka";

    /** Count of metadata markers received name. */
    public static final String MARKERS_RCVD_CNT = "MarkersCount";

    /** Count of metadata markers received description. */
    public static final String MARKERS_RCVD_CNT_DESC = "Count of metadata markers received from Kafka";

    /** Count of events sent name. */
    public static final String MSGS_SENT_CNT = "EventsSentCount";

    /** Count of events sent description. */
    public static final String MSGS_SENT_CNT_DESC = "Count of events sent to destination cluster";

    /** Timestamp of last sent batch name. */
    public static final String LAST_MSG_SENT_TIME = "LastBatchSentTime";

    /** Timestamp of last sent batch description. */
    public static final String LAST_MSG_SENT_TIME_DESC = "Timestamp of last sent batch to the destination cluster";

    /** Timestamp of last received message. */
    private AtomicLongMetric lastRcvdEvtTs;

    /** Count of received events. */
    private AtomicLongMetric evtsRcvdCnt;

    /** Timestamp of last sent message. */
    private AtomicLongMetric lastSntMsgTs;

    /** Count of sent events. */
    private AtomicLongMetric evtsSntCnt;

    /** Count of received markers. */
    private AtomicLongMetric markersCnt;

    /** Standalone kernal context. */
    private StandaloneGridKernalContext kctx;

    /** CDC metrics registry. */
    private MetricRegistryImpl mreg;

    /** */
    private final IgniteLogger log;

    /** Streamer configuration. */
    private final KafkaToIgniteCdcStreamerConfiguration streamerCfg;

    /** Metric registry manager. */
    private SingleMetricRegistryManager mregMgr;

    /** */
    private KafkaToIgniteMetrics(
        IgniteLogger log,
        KafkaToIgniteCdcStreamerConfiguration streamerCfg
    ) throws IgniteCheckedException {
        this.log = log;
        this.streamerCfg = streamerCfg;

        initStandaloneMetricsKernal();
        initMetrics();
    }

    /**
     * Creates an instance of {@link KafkaToIgniteMetrics}.
     * @param log Logger.
     * @param streamerCfg Streamer config.
     * @return {@link KafkaToIgniteMetrics} instance.
     */
    public static KafkaToIgniteMetrics startMetrics(
        IgniteLogger log,
        KafkaToIgniteCdcStreamerConfiguration streamerCfg
    ) {
        try {
            return new KafkaToIgniteMetrics(log, streamerCfg);
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /** @throws IgniteCheckedException If failed. */
    private void initStandaloneMetricsKernal() throws IgniteCheckedException {
        kctx = new StandaloneGridKernalContext(log, null, null) {
            @Override protected IgniteConfiguration prepareIgniteConfiguration() {
                IgniteConfiguration cfg = super.prepareIgniteConfiguration();

                cfg.setIgniteInstanceName(streamerCfg.getMetricRegistryName());

                if (!F.isEmpty(streamerCfg.getMetricExporterSpi()))
                    cfg.setMetricExporterSpi(streamerCfg.getMetricExporterSpi());
                else {
                    cfg.setMetricExporterSpi(U.IGNITE_MBEANS_DISABLED
                        ? new NoopMetricExporterSpi()
                        : new JmxMetricExporterSpi());
                }

                initializeDefaultMBeanServer(cfg);

                return cfg;
            }

            /** {@inheritDoc} */
            @Override public String igniteInstanceName() {
                return config().getIgniteInstanceName();
            }
        };

        mreg = new MetricRegistryImpl(metricName("cdc", "applier"), null, null, log);

        mregMgr = new SingleMetricRegistryManager(mreg);

        for (MetricExporterSpi exporterSpi : kctx.config().getMetricExporterSpi()) {
            kctx.resource().injectGeneric(exporterSpi);
            exporterSpi.setMetricRegistry(mregMgr);
            exporterSpi.onContextInitialized(new StandaloneSpiContext());
            exporterSpi.spiStart(null);
        }
    }

    /** Initialize metrics. */
    private void initMetrics() {
        this.evtsRcvdCnt = mreg.longMetric(EVTS_RCVD_CNT, EVTS_RCVD_CNT_DESC);
        this.lastRcvdEvtTs = mreg.longMetric(LAST_EVT_RCVD_TIME, LAST_EVT_RCVD_TIME_DESC);
        this.evtsSntCnt = mreg.longMetric(MSGS_SENT_CNT, MSGS_SENT_CNT_DESC);
        this.lastSntMsgTs = mreg.longMetric(LAST_MSG_SENT_TIME, LAST_MSG_SENT_TIME_DESC);
        this.markersCnt = mreg.longMetric(MARKERS_RCVD_CNT, MARKERS_RCVD_CNT_DESC);
    }

    /**
     * Stops metric manager and metrics SPI.
     */
    public void stopMetrics() {
        mregMgr.stop();

        for (MetricExporterSpi exporterSpi : kctx.config().getMetricExporterSpi())
            exporterSpi.spiStop();
    }

    /**
     * Increments the number of received messages from kafka.
     */
    public void incrementReceivedEvents() {
        this.evtsRcvdCnt.increment();
        this.lastRcvdEvtTs.value(System.currentTimeMillis());
    }

    /**
     * Adds count to total number of set messages to destination cluster.
     * @param cnt Count.
     */
    public void addSentEvents(int cnt) {
        this.evtsSntCnt.add(cnt);
        this.lastSntMsgTs.value(System.currentTimeMillis());
    }

    /** Increments the number of markers received from kafka. */
    public void incrementMarkers() {
        this.markersCnt.increment();
    }

    /** */
    private static class SingleMetricRegistryManager implements ReadOnlyMetricManager {
        /** */
        private final ReadOnlyMetricRegistry mreg;

        /** */
        List<Consumer<ReadOnlyMetricRegistry>> removeLsnrs = new ArrayList<>();

        /** */
        private SingleMetricRegistryManager(ReadOnlyMetricRegistry mreg) {
            this.mreg = mreg;
        }

        /** {@inheritDoc} */
        @Override public void addMetricRegistryCreationListener(Consumer<ReadOnlyMetricRegistry> lsnr) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void addMetricRegistryRemoveListener(Consumer<ReadOnlyMetricRegistry> lsnr) {
            removeLsnrs.add(lsnr);
        }

        /** */
        public void stop() {
            removeLsnrs.forEach(lsnr -> lsnr.accept(mreg));
        }

        /** {@inheritDoc} */
        @Override public Iterator<ReadOnlyMetricRegistry> iterator() {
            return Collections.singleton(mreg).iterator();
        }
    }
}
