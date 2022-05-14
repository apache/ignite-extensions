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
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.conflictresolve.CacheConflictResolutionManagerImpl;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverImpl;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import static org.apache.ignite.internal.IgniteKernal.NL;
import static org.apache.ignite.internal.IgniteKernal.SITE;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * Main class of Kafka to Ignite application.
 * This application is counterpart of {@link IgniteToKafkaCdcStreamer} Change Data Capture consumer.
 * Application runs several {@link KafkaToIgniteCdcStreamerApplier} thread to read Kafka topic partitions
 * and apply {@link CdcEvent} to Ignite.
 * <p>
 * Each applier receive even number of kafka topic partition to read.
 * <p>
 * In case of any error during read applier just fail. Fail of any applier will lead to the fail of whole application.
 * It expected that application will be configured for automatic restarts with the OS tool to failover temporary errors
 * such as Kafka or Ignite unavailability.
 * <p>
 * To resolve possible update conflicts (in case of concurrent update in source and destination Ignite clusters)
 * real-world deployments should use some conflict resolver, for example {@link CacheVersionConflictResolverImpl}.
 * Example of Ignite configuration with the conflict resolver:
 * <pre>
 * {@code
 * CacheVersionConflictResolverCachePluginProvider conflictPlugin = new CacheVersionConflictResolverCachePluginProvider();
 *
 * conflictPlugin.setClusterId(clusterId); // Cluster id.
 * conflictPlugin.setCaches(new HashSet<>(Arrays.asList("my-cache", "some-other-cache"))); // Caches to replicate.
 *
 * IgniteConfiguration cfg = ...;
 *
 * cfg.setPluginProviders(conflictPlugin);
 * }
 * </pre>
 * Please, see {@link CacheConflictResolutionManagerImpl} for additional information.
 *
 * @see CdcMain
 * @see IgniteToKafkaCdcStreamer
 * @see CdcEvent
 * @see KafkaToIgniteCdcStreamerApplier
 * @see CacheConflictResolutionManagerImpl
 */
@IgniteExperimental
public class KafkaToIgniteCdcStreamer implements Runnable {
    /** Ignite configuration. */
    private final IgniteConfiguration igniteCfg;

    /** Kafka consumer properties. */
    private final Properties kafkaProps;

    /** Streamer configuration. */
    private final KafkaToIgniteCdcStreamerConfiguration streamerCfg;

    /** Runners to run {@link KafkaToIgniteCdcStreamerApplier} instances. */
    private final List<Thread> runners;

    /** Appliers. */
    private final List<AutoCloseable> appliers;

    /**
     * @param igniteCfg Ignite configuration.
     * @param kafkaProps Kafka properties.
     * @param streamerCfg Streamer configuration.
     */
    public KafkaToIgniteCdcStreamer(
        IgniteConfiguration igniteCfg,
        Properties kafkaProps,
        KafkaToIgniteCdcStreamerConfiguration streamerCfg
    ) {
        A.notNull(streamerCfg.getTopic(), "Kafka topic");
        A.notNull(streamerCfg.getMetadataTopic(), "Kafka metadata topic");
        A.ensure(
            streamerCfg.getKafkaPartsFrom() >= 0,
            "The Kafka partitions lower bound must be explicitly set to a value greater than or equals to zero.");
        A.ensure(
            streamerCfg.getKafkaPartsTo() > 0,
            "The Kafka partitions upper bound must be explicitly set to a value greater than zero.");
        A.ensure(
            streamerCfg.getKafkaPartsTo() > streamerCfg.getKafkaPartsFrom(),
            "The Kafka partitions upper bound must be greater than lower bound.");
        A.ensure(streamerCfg.getKafkaRequestTimeout() >= 0, "The Kafka request timeout cannot be negative.");
        A.ensure(streamerCfg.getThreadCount() > 0, "Threads count value must me greater than zero.");
        A.ensure(
            streamerCfg.getKafkaPartsTo() - streamerCfg.getKafkaPartsFrom() >= streamerCfg.getThreadCount(),
            "Threads count must be less or equals to the total Kafka partitions count.");

        this.igniteCfg = igniteCfg;
        this.kafkaProps = kafkaProps;
        this.streamerCfg = streamerCfg;

        appliers = new ArrayList<>(streamerCfg.getThreadCount());
        runners = new ArrayList<>(streamerCfg.getThreadCount());

        if (!kafkaProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG))
            throw new IllegalArgumentException("Kafka properties don't contains " + ConsumerConfig.GROUP_ID_CONFIG);

        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try {
            runx();
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private void runx() throws Exception {
        U.initWorkDir(igniteCfg);

        IgniteLogger log = U.initLogger(igniteCfg, "kafka-ignite-streamer");

        igniteCfg.setGridLogger(log);

        ackAsciiLogo(log);

        try (IgniteEx ign = (IgniteEx)Ignition.start(igniteCfg)) {
            AtomicBoolean stopped = new AtomicBoolean();

            Set<Integer> caches = null;

            if (!F.isEmpty(streamerCfg.getCaches())) {
                caches = streamerCfg.getCaches().stream()
                    .peek(cache -> Objects.requireNonNull(ign.cache(cache), cache + " not exists!"))
                    .map(CU::cacheId).collect(Collectors.toSet());
            }

            KafkaToIgniteMetadataUpdater metaUpdr = new KafkaToIgniteMetadataUpdater(
                ign,
                log,
                kafkaProps,
                streamerCfg
            );

            int kafkaPartsFrom = streamerCfg.getKafkaPartsFrom();
            int kafkaParts = streamerCfg.getKafkaPartsTo() - kafkaPartsFrom;
            int threadCnt = streamerCfg.getThreadCount();

            int partPerApplier = kafkaParts / threadCnt;

            for (int i = 0; i < threadCnt; i++) {
                int from = i * partPerApplier;
                int to = (i + 1) * partPerApplier;

                if (i == threadCnt - 1)
                    to = kafkaParts;

                KafkaToIgniteCdcStreamerApplier applier = new KafkaToIgniteCdcStreamerApplier(
                    ign,
                    log,
                    kafkaProps,
                    streamerCfg.getTopic(),
                    kafkaPartsFrom + from,
                    kafkaPartsFrom + to,
                    caches,
                    streamerCfg.getMaxBatchSize(),
                    streamerCfg.getKafkaRequestTimeout(),
                    metaUpdr,
                    stopped
                );

                addAndStart("applier-thread-" + i, applier);
            }

            try {
                for (int i = 0; i < threadCnt + 1; i++)
                    runners.get(i).join();
            }
            catch (InterruptedException e) {
                stopped.set(true);

                appliers.forEach(U::closeQuiet);

                log.warning("Kafka to Ignite streamer interrupted", e);
            }
        }
    }

    /** Adds applier to {@link #appliers} and starts thread with it. */
    private <T extends AutoCloseable & Runnable> void addAndStart(String threadName, T applier) {
        appliers.add(applier);

        Thread thread = new Thread(applier, threadName);

        thread.start();

        runners.add(thread);
    }

    /** */
    private void ackAsciiLogo(IgniteLogger log) {
        String ver = "ver. " + ACK_VER_STR;

        if (log.isInfoEnabled()) {
            log.info(NL + NL +
                ">>>    __ _____   ______ _____     __________    __________  ________________" + NL +
                ">>>   / //_/ _ | / __/ //_/ _ |   /_  __/ __ \\  /  _/ ___/ |/ /  _/_  __/ __/" + NL +
                ">>>  / ,< / __ |/ _// ,< / __ |    / / / /_/ / _/ // (_ /    // /  / / / _/  " + NL +
                ">>> /_/|_/_/ |_/_/ /_/|_/_/ |_|   /_/  \\____/ /___/\\___/_/|_/___/ /_/ /___/  " + NL +
                ">>> " + NL +
                ">>> " + NL +
                ">>> " + ver + NL +
                ">>> " + COPYRIGHT + NL +
                ">>> " + NL +
                ">>> Ignite documentation: " + "http://" + SITE + NL +
                ">>> Kafka topic: " + streamerCfg.getTopic() + NL +
                ">>> Kafka partitions: " + streamerCfg.getKafkaPartsFrom() + "-" + streamerCfg.getKafkaPartsTo() + NL
            );
        }

        if (log.isQuiet()) {
            U.quiet(false,
                "   __ _____   ______ _____     __________    __________  ________________",
                "  / //_/ _ | / __/ //_/ _ |   /_  __/ __ \\  /  _/ ___/ |/ /  _/_  __/ __/",
                " / ,< / __ |/ _// ,< / __ |    / / / /_/ / _/ // (_ /    // /  / / / _/  ",
                "/_/|_/_/ |_/_/ /_/|_/_/ |_|   /_/  \\____/ /___/\\___/_/|_/___/ /_/ /___/  ",
                "",
                ver,
                COPYRIGHT,
                "",
                "Ignite documentation: " + "http://" + SITE,
                "Kafka topic: " + streamerCfg.getTopic(),
                "Kafka partitions: " + streamerCfg.getKafkaPartsFrom() + "-" + streamerCfg.getKafkaPartsTo(),
                "",
                "Quiet mode.");

            String fileName = log.fileName();

            if (fileName != null)
                U.quiet(false, "  ^-- Logging to file '" + fileName + '\'');

            if (log instanceof GridLoggerProxy)
                U.quiet(false, "  ^-- Logging by '" + ((GridLoggerProxy)log).getLoggerInfo() + '\'');

            U.quiet(false,
                "  ^-- To see **FULL** console log here add -DIGNITE_QUIET=false or \"-v\" to kafka-to-ignite.{sh|bat}",
                "");
        }
    }
}
