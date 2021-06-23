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
import org.apache.ignite.cdc.AbstractReplicationTest;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.ClassRule;

import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_PARTS;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_TOPIC;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests for kafka replication.
 */
public class CdcKafkaReplicationTest extends AbstractReplicationTest {
    /** */
    public static final String SRC_DEST_TOPIC = "source-dest";

    /** */
    public static final String DEST_SRC_TOPIC = "dest-source";

    /** */
    @ClassRule
    public static final EmbeddedKafkaCluster KAFKA = new EmbeddedKafkaCluster(1);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        KAFKA.start();

        KAFKA.createTopic(DFLT_TOPIC, DFLT_PARTS, 1);
        KAFKA.createTopic(SRC_DEST_TOPIC, DFLT_PARTS, 1);
        KAFKA.createTopic(DEST_SRC_TOPIC, DFLT_PARTS, 1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        KAFKA.deleteAllTopicsAndWait(getTestTimeout());
    }

    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActivePassiveCdc() {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < srcCluster.length; i++)
            futs.add(igniteToKafka(srcCluster[i].configuration(), DFLT_TOPIC, ACTIVE_PASSIVE_CACHE));

        for (int i = 0; i < destCluster.length; i++) {
            futs.add(kafkaToIgnite(
                ACTIVE_PASSIVE_CACHE,
                DFLT_TOPIC,
                destClusterCliCfg[i],
                i * (DFLT_PARTS / 2),
                (i + 1) * (DFLT_PARTS / 2)
            ));
        }


        return futs;
    }

    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActiveActiveCdc() {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < srcCluster.length; i++)
            futs.add(igniteToKafka(srcCluster[i].configuration(), SRC_DEST_TOPIC, AbstractReplicationTest.ACTIVE_ACTIVE_CACHE));

        for (int i = 0; i < destCluster.length; i++)
            futs.add(igniteToKafka(destCluster[i].configuration(), DEST_SRC_TOPIC, AbstractReplicationTest.ACTIVE_ACTIVE_CACHE));

        futs.add(kafkaToIgnite(ACTIVE_ACTIVE_CACHE, SRC_DEST_TOPIC, destClusterCliCfg[0], 0, DFLT_PARTS));
        futs.add(kafkaToIgnite(ACTIVE_ACTIVE_CACHE, DEST_SRC_TOPIC, srcClusterCliCfg[0], 0, DFLT_PARTS));

        return futs;
    }

    /**
     * @param igniteCfg Ignite configuration.
     * @param topic Kafka topic name.
     * @param cache Cache name to stream to kafka.
     * @return Future for Change Data Capture application.
     */
    protected IgniteInternalFuture<?> igniteToKafka(IgniteConfiguration igniteCfg, String topic, String cache) {
        return runAsync(() -> {
            IgniteToKafkaCdcStreamer cdcCnsmr =
                new IgniteToKafkaCdcStreamer(topic, DFLT_PARTS, Collections.singleton(cache), KEYS_CNT, false, kafkaProperties());

            CdcConfiguration cdcCfg = new CdcConfiguration();

            cdcCfg.setConsumer(cdcCnsmr);

            new CdcMain(igniteCfg, null, cdcCfg).run();
        });
    }

    /**
     * @param cacheName Cache name.
     * @param igniteCfg Ignite configuration.
     * @return Future for runed {@link KafkaToIgniteCdcStreamer}.
     */
    protected IgniteInternalFuture<?> kafkaToIgnite(
        String cacheName,
        String topic,
        IgniteConfiguration igniteCfg,
        int fromPart,
        int toPart
    ) {
        KafkaToIgniteCdcStreamerConfiguration cfg = new KafkaToIgniteCdcStreamerConfiguration();

        cfg.setKafkaPartsFrom(fromPart);
        cfg.setKafkaPartsTo(toPart);
        cfg.setThreadCount((toPart - fromPart)/2);

        cfg.setCaches(Collections.singletonList(cacheName));
        cfg.setTopic(topic);

        return runAsync(new KafkaToIgniteCdcStreamer(igniteCfg, kafkaProperties(), cfg));
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
