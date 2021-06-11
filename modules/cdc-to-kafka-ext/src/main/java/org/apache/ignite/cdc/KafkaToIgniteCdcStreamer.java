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

package org.apache.ignite.cdc;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cdc.conflictresolve.CacheConflictResolutionManagerImpl;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverImpl;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.ChangeDataCapture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.jetbrains.annotations.NotNull;

/**
 * Main class of Kafka to Ignite application.
 * This application is counterpart of {@link IgniteToKafkaCdcStreamer} Change Data Capture consumer.
 * Application runs several {@link Applier} thread to read Kafka topic partitions and apply {@link ChangeDataCaptureEvent} to Ignite.
 * <p>
 * Each applier receive even number of kafka topic partition to read.
 * <p>
 * In case of any error during read applier just fail. Fail of any applier will lead to the fail of whole application.
 * It expected that application will be configured for automatic restarts with the OS tool to failover temporary errors
 * such as Kafka or Ignite unavailability.
 * <p>
 * To resolve possible update conflicts(in case of concurrent update in source and destination Ignite clusters)
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
 * @see ChangeDataCapture
 * @see IgniteToKafkaCdcStreamer
 * @see ChangeDataCaptureEvent
 * @see Applier
 * @see CacheConflictResolutionManagerImpl
 */
public class KafkaToIgniteCdcStreamer implements Runnable {
    /** Ignite configuration. */
    private final IgniteConfiguration igniteCfg;

    /** Kafka consumer properties. */
    private final Properties kafkaProps;

    /** Streamer configuration. */
    private final KafkaToIgniteCdcStreamerConfiguration streamerCfg;

    /** Executor service to run {@link Applier} instances. */
    private final ExecutorService execSvc;

    /** Appliers. */
    private final List<Applier> appliers;

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
        this.igniteCfg = igniteCfg;
        this.kafkaProps = kafkaProps;
        this.streamerCfg = streamerCfg;

        appliers = new ArrayList<>(streamerCfg.getThreadCount());

        execSvc = Executors.newFixedThreadPool(streamerCfg.getThreadCount(), new ThreadFactory() {
            private final AtomicInteger cntr = new AtomicInteger();

            @Override public Thread newThread(@NotNull Runnable r) {
                Thread th = new Thread(r);

                th.setName("applier-thread-" + cntr.getAndIncrement());

                return th;
            }
        });

        if (!kafkaProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG))
            throw new IllegalArgumentException("Kafka properties don't contains " + ConsumerConfig.GROUP_ID_CONFIG);

        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try (IgniteEx ign = (IgniteEx)Ignition.start(igniteCfg)) {
            IgniteLogger log = U.initLogger(
                igniteCfg.getGridLogger(),
                "kafka-ignite-streamer",
                igniteCfg.getNodeId() != null ? igniteCfg.getNodeId() : UUID.randomUUID(),
                igniteCfg.getWorkDirectory()
            );

            AtomicBoolean closed = new AtomicBoolean();

            Set<Integer> caches = null;

            if (!F.isEmpty(streamerCfg.getCacheNames())) {
                caches = streamerCfg.getCacheNames().stream()
                    .peek(cache -> Objects.requireNonNull(ign.cache(cache), cache + " not exists!"))
                    .map(CU::cacheId).collect(Collectors.toSet());
            }

            int partPerApplier = streamerCfg.getKafkaPartitions() / streamerCfg.getThreadCount();

            for (int i = 0; i < streamerCfg.getThreadCount(); i++) {
                int from = i * partPerApplier;

                int to = (i == streamerCfg.getThreadCount() - 1)
                    ? streamerCfg.getKafkaPartitions() + 1
                    : (i + 1) * partPerApplier;

                appliers.add(new Applier(
                    ign,
                    log,
                    kafkaProps,
                    streamerCfg.getTopic(),
                    from,
                    to,
                    caches,
                    streamerCfg.getMaxBatchSize(),
                    closed
                ));
            }

            try {
                appliers.forEach(execSvc::submit);

                execSvc.shutdown();

                boolean stopped = execSvc.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

                assert stopped;
            }
            catch (InterruptedException e) {
                closed.set(true);

                appliers.forEach(U::closeQuiet);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
