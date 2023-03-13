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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cdc.AbstractReplicationTest;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.cdc.ConflictResolvableTestData;
import org.apache.ignite.cdc.IgniteToIgniteCdcStreamer;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.logging.log4j.Level;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_KAFKA_REQ_TIMEOUT;
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
    private static final LogListener logListener = LogListener.matches("Offsets unchanged, poll skipped").build();

    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (KAFKA == null) {
            KAFKA = new EmbeddedKafkaCluster(1);

            KAFKA.start();
        }

        KAFKA.createTopic(SRC_DEST_TOPIC, DFLT_PARTS, 1);
        KAFKA.createTopic(DEST_SRC_TOPIC, DFLT_PARTS, 1);
        KAFKA.createTopic(SRC_DEST_META_TOPIC, 1, 1);
        KAFKA.createTopic(DEST_SRC_META_TOPIC, 1, 1);

        testLog.registerListener(logListener);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        KAFKA.getAllTopicsInCluster().forEach(t -> {
            try {
                KAFKA.deleteTopic(t);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        waitForCondition(() -> KAFKA.getAllTopicsInCluster().isEmpty(), getTestTimeout());

        testLog.clearListeners();
        logListener.reset();
    }

    /** */
    @Test
    public void testMetadataUpdateSkipped() throws Exception {
        // Skip test in sublass.
        Assume.assumeTrue(getClass().isAssignableFrom(CdcKafkaReplicationTest.class));

        resetLog4j(Level.DEBUG, false, KafkaToIgniteMetadataUpdater.class.getCanonicalName());

        IgniteCache<Integer, ConflictResolvableTestData> srcCache = createCache(srcCluster[0], ACTIVE_PASSIVE_CACHE);
        IgniteCache<Integer, ConflictResolvableTestData> destCache = createCache(destCluster[0], ACTIVE_PASSIVE_CACHE);

        assertFalse("Unexpected metadata skip messages", logListener.check());

        List<IgniteInternalFuture<?>> futs = startActivePassiveCdc(ACTIVE_PASSIVE_CACHE);

        try {
            srcCache.putAll(F.asMap(0, ConflictResolvableTestData.create()));

            assertTrue(waitForCondition(() -> destCache.containsKey(0), getTestTimeout()));

            assertTrue("Metadata update was not skipped", logListener.check());
        }
        finally {
            for (IgniteInternalFuture<?> fut : futs)
                fut.cancel();
        }
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

        for (IgniteEx ex : srcCluster)
            futs.add(igniteToKafka(ex.configuration(), cache, SRC_DEST_META_TOPIC, cache));

        for (int i = 0; i < destCluster.length; i++) {
            futs.add(kafkaToIgnite(
                cache,
                cache,
                SRC_DEST_META_TOPIC,
                destClusterCliCfg[i],
                destCluster,
                i * (DFLT_PARTS / 2),
                (i + 1) * (DFLT_PARTS / 2)
            ));
        }

        return futs;
    }

    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActiveActiveCdc() {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (IgniteEx ex : srcCluster)
            futs.add(igniteToKafka(ex.configuration(), SRC_DEST_TOPIC, SRC_DEST_META_TOPIC, ACTIVE_ACTIVE_CACHE));

        for (IgniteEx ex : destCluster)
            futs.add(igniteToKafka(ex.configuration(), DEST_SRC_TOPIC, DEST_SRC_META_TOPIC, ACTIVE_ACTIVE_CACHE));

        futs.add(kafkaToIgnite(
            ACTIVE_ACTIVE_CACHE,
            SRC_DEST_TOPIC,
            SRC_DEST_META_TOPIC,
            destClusterCliCfg[0],
            destCluster,
            0,
            DFLT_PARTS
        ));

        futs.add(kafkaToIgnite(
            ACTIVE_ACTIVE_CACHE,
            DEST_SRC_TOPIC,
            DEST_SRC_META_TOPIC,
            srcClusterCliCfg[0],
            srcCluster,
            0,
            DFLT_PARTS
        ));

        return futs;
    }

    /** {@inheritDoc} */
    @Override protected void checkConsumerMetrics(Function<String, Long> longMetric) {
        assertNotNull(longMetric.apply(IgniteToIgniteCdcStreamer.LAST_EVT_TIME));
        assertNotNull(longMetric.apply(IgniteToIgniteCdcStreamer.EVTS_CNT));
        assertNotNull(longMetric.apply(IgniteToKafkaCdcStreamer.BYTES_SENT));
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
        String cache
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
        });
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
        int toPart
    ) {
        KafkaToIgniteCdcStreamerConfiguration cfg = new KafkaToIgniteCdcStreamerConfiguration();

        cfg.setKafkaPartsFrom(fromPart);
        cfg.setKafkaPartsTo(toPart);
        cfg.setThreadCount((toPart - fromPart) / 2);

        cfg.setCaches(Collections.singletonList(cacheName));
        cfg.setTopic(topic);
        cfg.setMetadataTopic(metadataTopic);
        cfg.setKafkaRequestTimeout(DFLT_KAFKA_REQ_TIMEOUT);

        AbstractKafkaToIgniteCdcStreamer streamer;

        if (clientType == ClientType.THIN_CLIENT) {
            ClientConfiguration clientCfg = new ClientConfiguration();

            clientCfg.setAddresses(hostAddresses(dest));

            streamer = new KafkaToIgniteClientCdcStreamer(clientCfg, kafkaProperties(), cfg);
        }
        else
            streamer = new KafkaToIgniteCdcStreamer(igniteCfg, kafkaProperties(), cfg);

        streamer.logger(testLog);

        return runAsync(streamer);
    }

    /** */
    protected Properties kafkaProperties() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-ignite-applier");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

        return props;
    }
}
