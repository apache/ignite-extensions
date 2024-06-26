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

package org.apache.ignite.cdc.metrics;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneSpiContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;

import static org.apache.ignite.internal.IgnitionEx.initializeDefaultMBeanServer;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext.closeAllComponents;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext.startAllComponents;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/** CDC kafka to ignite metrics. */
public class KafkaToIgniteMetrics {
    /** Count of events received name. */
    public static final String K2I_EVTS_RSVD_CNT = "EventsReceivedCount";

    /** Count of events received description. */
    public static final String K2I_EVTS_RSVD_CNT_DESC = "Count of events received from kafka";

    /** Timestamp of last received event name. */
    public static final String K2I_LAST_EVT_RSVD_TIME = "LastEventReceivedTime";

    /** Timestamp of last received event description. */
    public static final String K2I_LAST_EVT_RSVD_TIME_DESC = "Timestamp of last received event from kafka";

    /** Count of metadata markers received name. */
    public static final String K2I_MARKERS_RSVD_CNT = "MarkersCount";

    /** Count of metadata markers received description. */
    public static final String K2I_MARKERS_RSVD_CNT_DESC = "Count of metadata markers received from Kafka";

    /** Count of events sent name. */
    public static final String K2I_MSGS_SNT_CNT = "EventsSentCount";

    /** Count of events sent description. */
    public static final String K2I_MSGS_SNT_CNT_DESC = "Count of events sent to destination cluster";

    /** Timestamp of last sent batch name. */
    public static final String K2I_LAST_MSG_SNT_TIME = "LastBatchSentTime";

    /** Timestamp of last sent batch description. */
    public static final String K2I_LAST_MSG_SNT_TIME_DESC = "Timestamp of last sent batch to the destination cluster";

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

    /** Ignite instance name. */
    private final String igniteInstanceName;

    /** */
    private KafkaToIgniteMetrics(
        IgniteLogger log,
        KafkaToIgniteCdcStreamerConfiguration streamerCfg,
        String igniteInstanceName
    ) throws IgniteCheckedException {
        this.log = log;
        this.streamerCfg = streamerCfg;
        this.igniteInstanceName = igniteInstanceName;

        initStandaloneMetricsKernal();
        initMetrics();
    }

    /**
     * Creates an instance of {@link KafkaToIgniteMetrics}.
     * @param log Logger.
     * @param streamerCfg Streamer config.
     * @param igniteInstanceName Ignite instance name.
     * @return {@link KafkaToIgniteMetrics} instance.
     */
    public static KafkaToIgniteMetrics startMetrics(
        IgniteLogger log,
        KafkaToIgniteCdcStreamerConfiguration streamerCfg,
        String igniteInstanceName
    ) {
        try {
            return new KafkaToIgniteMetrics(log, streamerCfg, igniteInstanceName);
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

                cfg.setIgniteInstanceName("kafka-ignite-streamer-" + igniteInstanceName);

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

        startAllComponents(kctx);

        for (IgniteSpi metricSpi : kctx.config().getMetricExporterSpi())
            metricSpi.onContextInitialized(new StandaloneSpiContext());
    }

    /** Initialize metrics. */
    private void initMetrics() {
        mreg = kctx.metric().registry(metricName("kafka-to-ignite-metrics"));

        this.evtsRcvdCnt = mreg.longMetric(K2I_EVTS_RSVD_CNT, K2I_EVTS_RSVD_CNT_DESC);
        this.lastRcvdEvtTs = mreg.longMetric(K2I_LAST_EVT_RSVD_TIME, K2I_LAST_EVT_RSVD_TIME_DESC);
        this.evtsSntCnt = mreg.longMetric(K2I_MSGS_SNT_CNT, K2I_MSGS_SNT_CNT_DESC);
        this.lastSntMsgTs = mreg.longMetric(K2I_LAST_MSG_SNT_TIME, K2I_LAST_MSG_SNT_TIME_DESC);
        this.markersCnt = mreg.longMetric(K2I_MARKERS_RSVD_CNT, K2I_MARKERS_RSVD_CNT_DESC);
    }

    /**
     * Stops metric manager and metrics SPI.
     */
    public void stopMetrics() throws IgniteCheckedException {
        if (kctx != null)
            closeAllComponents(kctx);
    }

    /**
     * Adds count to total number of received messages from kafka.
     * @param cnt Count.
     */
    public void addReceivedEvents(int cnt) {
        this.evtsRcvdCnt.add(cnt);
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

    /** Decrements the number of events received from kafka. */
    public void decrementReceivedEvents() {
        this.evtsRcvdCnt.decrement();
    }
}
