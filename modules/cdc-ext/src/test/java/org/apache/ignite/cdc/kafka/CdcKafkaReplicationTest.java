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
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.management.DynamicMBean;
import org.apache.ignite.cdc.AbstractReplicationTest;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;

import static org.apache.ignite.cdc.AbstractIgniteCdcStreamer.EVTS_SENT_CNT;
import static org.apache.ignite.cdc.AbstractIgniteCdcStreamer.LAST_EVT_SENT_TIME;
import static org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer.BYTES_SENT_CNT;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_KAFKA_REQ_TIMEOUT;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_METRICS_REG_NAME;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteMetrics.EVTS_RCVD_CNT;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteMetrics.LAST_EVT_RCVD_TIME;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteMetrics.LAST_MSG_SENT_TIME;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteMetrics.MSGS_SENT_CNT;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests for kafka replication.
 */
public class CdcKafkaReplicationTest extends AbstractReplicationTest {
    /** */
    public static final String SRC_DEST_TOPIC = "source-dest";

    /** */
    public static final String DEST_SRC_TOPIC = "dest-source";

    /** */
    public static final String SRC_DEST_META_TOPIC = "source-dest-meta";

    /** */
    public static final String DEST_SRC_META_TOPIC = "dest-source-meta";

    /** */
    public static final int DFLT_PARTS = 16;

    /** */
    private static EmbeddedKafkaCluster KAFKA = null;

    /** */
    protected List<AbstractKafkaToIgniteCdcStreamer> kafkaStreamers;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        KAFKA = initKafka(KAFKA);
        kafkaStreamers = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        removeKafkaTopicsAndWait(KAFKA, getTestTimeout());
    }

    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActivePassiveCdc(String cache) {
        try {
            KAFKA.createTopic(cache, DFLT_PARTS, 1);

            waitForCondition(() -> KAFKA.getAllTopicsInCluster().contains(cache), getTestTimeout());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (IgniteEx ex : srcCluster) {
            int idx = getTestIgniteInstanceIndex(ex.name());

            futs.add(igniteToKafka(ex.configuration(), cache, SRC_DEST_META_TOPIC, cache, "ignite-src-to-kafka-" + idx));
        }

        for (int i = 0; i < destCluster.length; i++) {
            futs.add(kafkaToIgnite(
                cache,
                cache,
                SRC_DEST_META_TOPIC,
                destClusterCliCfg[i],
                destCluster,
                i * (DFLT_PARTS / 2),
                (i + 1) * (DFLT_PARTS / 2),
                 "kafka-to-ignite-dest-" + i
            ));
        }

        return futs;
    }

    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActiveActiveCdc() {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (IgniteEx ex : srcCluster) {
            int idx = getTestIgniteInstanceIndex(ex.name());

            futs.add(igniteToKafka(
                    ex.configuration(), SRC_DEST_TOPIC, SRC_DEST_META_TOPIC, ACTIVE_ACTIVE_CACHE, "ignite-src-to-kafka-" + idx));
        }

        for (IgniteEx ex : destCluster) {
            int idx = getTestIgniteInstanceIndex(ex.name());

            futs.add(igniteToKafka(
                    ex.configuration(), DEST_SRC_TOPIC, DEST_SRC_META_TOPIC, ACTIVE_ACTIVE_CACHE, "ignite-dest-to-kafka-" + idx));
        }

        futs.add(kafkaToIgnite(
            ACTIVE_ACTIVE_CACHE,
            SRC_DEST_TOPIC,
            SRC_DEST_META_TOPIC,
            destClusterCliCfg[0],
            destCluster,
            0,
            DFLT_PARTS,
            "kafka-to-ignite-src"
        ));

        futs.add(kafkaToIgnite(
            ACTIVE_ACTIVE_CACHE,
            DEST_SRC_TOPIC,
            DEST_SRC_META_TOPIC,
            srcClusterCliCfg[0],
            srcCluster,
            0,
            DFLT_PARTS,
            "kafka-to-ignite-dest"
        ));

        return futs;
    }

    /** {@inheritDoc} */
    @Override protected void checkMetrics() throws IgniteInterruptedCheckedException {
        super.checkMetrics();

        for (AbstractKafkaToIgniteCdcStreamer streamer : kafkaStreamers) {
            KafkaToIgniteMetrics metrics = getFieldValue(streamer, "metrics");
            MetricRegistryImpl mreg = getFieldValue(metrics, "mreg");
            checkK2IMetrics(m -> mreg.<AtomicLongMetric>findMetric(m).value());
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkConsumerMetrics(Function<String, Long> longMetric) {
        assertNotNull(longMetric.apply(LAST_EVT_SENT_TIME));
        assertNotNull(longMetric.apply(EVTS_SENT_CNT));
        assertNotNull(longMetric.apply(BYTES_SENT_CNT));
    }

    /** {@inheritDoc} */
    @Override protected void checkMetricsCount(int evtsCnt) {
        checkMetricsEventsCount(evtsCnt, getConsumerEventsCount(EVTS_SENT_CNT));

        checkMetricsEventsCount(evtsCnt, getKafkaApplierEventsCount(EVTS_RCVD_CNT));
        checkMetricsEventsCount(evtsCnt, getKafkaApplierEventsCount(MSGS_SENT_CNT));

        checkMetricsEventsCount(evtsCnt, getKafkaApplierEventsCountJmx(EVTS_RCVD_CNT));
        checkMetricsEventsCount(evtsCnt, getKafkaApplierEventsCountJmx(MSGS_SENT_CNT));
    }

    /**
     * Returns metric for events from kafka CDC consumer using reflection API.
     * @param metricName Metric name.
     */
    protected Supplier<Long> getKafkaApplierEventsCount(String metricName) {
        return () -> {
            long cnt = 0;

            for (AbstractKafkaToIgniteCdcStreamer streamer : kafkaStreamers) {
                KafkaToIgniteMetrics metrics = getFieldValue(streamer, "metrics");
                MetricRegistryImpl mreg = getFieldValue(metrics, "mreg");
                Function<String, Long> longMetric = m -> mreg.<AtomicLongMetric>findMetric(m).value();

                cnt += longMetric.apply(metricName);
            }

            return cnt;
        };
    }

    /**
     * Returns metric for events from kafka CDC consumer using jmx.
     * @param metricName Metric name.
     */
    protected Supplier<Long> getKafkaApplierEventsCountJmx(String metricName) {
        return () -> {
            long cnt = 0;

            for (AbstractKafkaToIgniteCdcStreamer streamer : kafkaStreamers) {
                KafkaToIgniteCdcStreamerConfiguration streamerCfg = getFieldValue(streamer, "streamerCfg");

                DynamicMBean jmxApplierReg = metricRegistry(streamerCfg.getMetricRegistryName(), "cdc", "applier");

                cnt += ((Function<String, Long>)jmxVal(jmxApplierReg)).apply(metricName);
            }

            return cnt;
        };
    }

    /**
     * Checks metrics for Kafka To Ignite consumer
     * @param longMetric Long metric.
     */
    private void checkK2IMetrics(Function<String, Long> longMetric) {
        assertNotNull(longMetric.apply(LAST_EVT_RCVD_TIME));
        assertNotNull(longMetric.apply(EVTS_RCVD_CNT));

        assertNotNull(longMetric.apply(LAST_MSG_SENT_TIME));
        assertNotNull(longMetric.apply(MSGS_SENT_CNT));
    }

    /**
     * @param igniteCfg Ignite configuration.
     * @param topic Kafka topic name.
     * @param metadataTopic Metadata topic name.
     * @param cache Cache name to stream to kafka.
     * @return Future for Change Data Capture application.
     */
    protected IgniteInternalFuture<?> igniteToKafka(
        IgniteConfiguration igniteCfg,
        String topic,
        String metadataTopic,
        String cache,
        String threadName
    ) {
        return runAsync(() -> {
            IgniteToKafkaCdcStreamer cdcCnsmr = new IgniteToKafkaCdcStreamer()
                .setTopic(topic)
                .setMetadataTopic(metadataTopic)
                .setKafkaPartitions(DFLT_PARTS)
                .setCaches(Collections.singleton(cache))
                .setMaxBatchSize(KEYS_CNT)
                .setOnlyPrimary(false)
                .setKafkaProperties(kafkaProperties())
                .setKafkaRequestTimeout(DFLT_KAFKA_REQ_TIMEOUT);

            CdcConfiguration cdcCfg = new CdcConfiguration();

            cdcCfg.setConsumer(cdcCnsmr);
            cdcCfg.setMetricExporterSpi(new JmxMetricExporterSpi());

            CdcMain cdc = new CdcMain(igniteCfg, null, cdcCfg);

            cdcs.add(cdc);

            cdc.run();
        }, threadName);
    }

    /**
     * @param cacheName Cache name.
     * @param igniteCfg Ignite configuration.
     * @param dest Destination Ignite cluster.
     * @return Future for runed {@link KafkaToIgniteCdcStreamer}.
     */
    protected IgniteInternalFuture<?> kafkaToIgnite(
        String cacheName,
        String topic,
        String metadataTopic,
        IgniteConfiguration igniteCfg,
        IgniteEx[] dest,
        int fromPart,
        int toPart,
        String threadName
    ) {
        KafkaToIgniteCdcStreamerConfiguration cfg = new KafkaToIgniteCdcStreamerConfiguration();

        cfg.setKafkaPartsFrom(fromPart);
        cfg.setKafkaPartsTo(toPart);
        cfg.setThreadCount((toPart - fromPart) / 2);

        cfg.setCaches(Collections.singletonList(cacheName));
        cfg.setTopic(topic);
        cfg.setMetadataTopic(metadataTopic);
        cfg.setKafkaRequestTimeout(DFLT_KAFKA_REQ_TIMEOUT);

        cfg.setMetricRegistryName(DFLT_METRICS_REG_NAME + "-" + igniteCfg.getIgniteInstanceName());

        AbstractKafkaToIgniteCdcStreamer streamer;

        if (clientType == ClientType.THIN_CLIENT) {
            ClientConfiguration clientCfg = new ClientConfiguration();

            clientCfg.setAddresses(hostAddresses(dest));

            streamer = new KafkaToIgniteClientCdcStreamer(clientCfg, kafkaProperties(), cfg);
        }
        else
            streamer = new KafkaToIgniteCdcStreamer(igniteCfg, kafkaProperties(), cfg);

        kafkaStreamers.add(streamer);

        return runAsync(streamer, threadName);
    }

    /** */
    protected Properties kafkaProperties() {
        return kafkaProperties(KAFKA);
    }

    /**
     * @param kafka Kafka cluster.
     */
    static Properties kafkaProperties(EmbeddedKafkaCluster kafka) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-ignite-applier");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

        return props;
    }

    /**
     * Init Kafka cluster if current instance is null and create topics.
     *
     * @param curKafka Current kafka.
     */
    static EmbeddedKafkaCluster initKafka(EmbeddedKafkaCluster curKafka) throws Exception {
        EmbeddedKafkaCluster kafka = curKafka;

        if (kafka == null) {
            Properties props = new Properties();

            props.put("auto.create.topics.enable", "false");

            kafka = new EmbeddedKafkaCluster(1, props);

            kafka.start();
        }

        kafka.createTopic(SRC_DEST_TOPIC, DFLT_PARTS, 1);
        kafka.createTopic(DEST_SRC_TOPIC, DFLT_PARTS, 1);
        kafka.createTopic(SRC_DEST_META_TOPIC, 1, 1);
        kafka.createTopic(DEST_SRC_META_TOPIC, 1, 1);

        return kafka;
    }

    /**
     * @param kafka Kafka cluster.
     * @param timeout Timeout.
     */
    static void removeKafkaTopicsAndWait(EmbeddedKafkaCluster kafka, long timeout) throws IgniteInterruptedCheckedException {
        kafka.getAllTopicsInCluster().forEach(t -> {
            try {
                kafka.deleteTopic(t);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        waitForCondition(() -> kafka.getAllTopicsInCluster().isEmpty(), timeout);
    }
}
