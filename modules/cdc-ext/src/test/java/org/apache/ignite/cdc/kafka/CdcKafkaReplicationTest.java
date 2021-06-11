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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cdc.ChangeDataCaptureConfiguration;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cdc.ChangeDataCapture;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer.DFLT_REQ_TIMEOUT;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_PARTS;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_TOPIC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests for kafka replication.
 */
@RunWith(Parameterized.class)
public class CdcKafkaReplicationTest extends GridCommonAbstractTest {
    /** Cache mode. */
    @Parameterized.Parameter
    public CacheAtomicityMode cacheMode;

    /** */
    @Parameterized.Parameter(1)
    public int backupCnt;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "cacheMode={0},backupCnt={1}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
            {ATOMIC, 0},
            {ATOMIC, 1},
            {TRANSACTIONAL, 0},
            {TRANSACTIONAL, 1}
        });
    }

    /** */
    public static final String SRC_DEST_TOPIC = "source-dest";

    /** */
    public static final String DEST_SRC_TOPIC = "dest-source";

    /** */
    public static final String AP_CACHE = "active-passive-cache";

    /** */
    public static final String ACTIVE_ACTIVE_CACHE = "active-active-cache";

    /** */
    public static final byte SRC_CLUSTER_ID = 26;

    /** */
    public static final byte DEST_CLUSTER_ID = 27;

    /** */
    public static final int BOTH_EXISTS = 1;

    /** */
    public static final int BOTH_REMOVED = 2;

    /** */
    public static final int ANY_SAME_STATE = 3;

    /** */
    public static final int KEYS_CNT = 50;

    /** */
    public static final AtomicLong REQUEST_ID = new AtomicLong();

    /** */
    private static Properties props;

    /** */
    private static IgniteEx[] srcCluster;

    /** */
    private static IgniteConfiguration srcClusterCliCfg;

    /** */
    private static IgniteEx[] destCluster;

    /** */
    private static IgniteConfiguration destClusterCliCfg;

    /** */
    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    /** */
    private int commPort = TcpCommunicationSpi.DFLT_PORT;

    /** */
    private int discoPort = TcpDiscoverySpi.DFLT_PORT;

    /** */
    private byte clusterId = SRC_CLUSTER_ID;

    /** */
    private static final ThreadLocal<FastCrc> crc = new ThreadLocal<>().withInitial(FastCrc::new);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheVersionConflictResolverPluginProvider<?> cfgPlugin = new CacheVersionConflictResolverPluginProvider<>();

        cfgPlugin.setClusterId(clusterId);
        cfgPlugin.setCaches(new HashSet<>(Collections.singletonList(ACTIVE_ACTIVE_CACHE)));
        cfgPlugin.setConflictResolveField("reqId");

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setLocalPort(discoPort)
                .setLocalPortRange(DFLT_PORT_RANGE)
                .setIpFinder(new TcpDiscoveryVmIpFinder() {{
                    setAddresses(Collections.singleton("127.0.0.1:" + discoPort + ".." + (discoPort + DFLT_PORT_RANGE)));
                }}))
            .setCommunicationSpi(new TcpCommunicationSpi()
                .setLocalPort(commPort)
                .setLocalPortRange(DFLT_PORT_RANGE))
            .setPluginProviders(cfgPlugin);

        if (!cfg.isClientMode()) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

            cfg.getDataStorageConfiguration()
                .setWalForceArchiveTimeout(5_000)
                .setChangeDataCaptureEnabled(true);

            cfg.setConsistentId(igniteInstanceName);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        kafka.start();

        if (props == null) {
            props = new Properties();

            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-ignite-applier");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        }

        createTopic(DFLT_TOPIC, DFLT_PARTS, props);
        createTopic(SRC_DEST_TOPIC, DFLT_PARTS, props);
        createTopic(DEST_SRC_TOPIC, DFLT_PARTS, props);

        srcCluster = new IgniteEx[] {
            startGrid(1),
            startGrid(2)
        };

        srcClusterCliCfg = optimize(getConfiguration("src-cluster-client").setClientMode(true));

        srcCluster[0].cluster().state(ACTIVE);
        srcCluster[0].cluster().tag("source");

        discoPort += DFLT_PORT_RANGE + 1;
        commPort += DFLT_PORT_RANGE + 1;
        clusterId = DEST_CLUSTER_ID;

        destCluster = new IgniteEx[] {
            startGrid(4),
            startGrid(5)
        };

        destClusterCliCfg = optimize(getConfiguration("dest-cluster-client").setClientMode(true));

        assertFalse("source".equals(destCluster[0].cluster().tag()));

        destCluster[0].cluster().state(ACTIVE);
        destCluster[0].cluster().tag("destination");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        props = null;

        stopAllGrids();

        cleanPersistenceDir();

        kafka.stop();
    }

    /** */
    @Test
    public void testActivePassiveReplication() throws Exception {
        IgniteInternalFuture<?> fut1 = igniteToKafka(srcCluster[0], DFLT_TOPIC, AP_CACHE);
        IgniteInternalFuture<?> fut2 = igniteToKafka(srcCluster[1], DFLT_TOPIC, AP_CACHE);

        try {
            IgniteCache<Integer, Data> destCache = destCluster[0].createCache(AP_CACHE);

            destCache.put(1, new Data(null, 0, 1, REQUEST_ID.incrementAndGet()));
            destCache.remove(1);

            runAsync(generateData("cache-1", srcCluster[srcCluster.length - 1], IntStream.range(0, KEYS_CNT), 1));
            runAsync(generateData(AP_CACHE, srcCluster[srcCluster.length - 1], IntStream.range(0, KEYS_CNT), 1));

            KafkaToIgniteCdcStreamerConfiguration cfg = new KafkaToIgniteCdcStreamerConfiguration();

            cfg.setCacheNames(Collections.singletonList(AP_CACHE));

            IgniteInternalFuture<?> k2iFut =
                runAsync(new KafkaToIgniteCdcStreamer(destClusterCliCfg, props, cfg));

            try {
                IgniteCache<Integer, Data> srcCache = srcCluster[srcCluster.length - 1].getOrCreateCache(AP_CACHE);

                waitForSameData(srcCache, destCache, KEYS_CNT, BOTH_EXISTS, 1, fut1, fut2, k2iFut);

                IntStream.range(0, KEYS_CNT).forEach(srcCache::remove);

                waitForSameData(srcCache, destCache, KEYS_CNT, BOTH_REMOVED, 1, fut1, fut2, k2iFut);
            }
            finally {
                k2iFut.cancel();
            }
        }
        finally {
            fut1.cancel();
            fut2.cancel();
        }
    }

    /** */
    @Test
    public void testActiveActiveReplication() throws Exception {
        IgniteCache<Integer, Data> srcCache = srcCluster[0].getOrCreateCache(ACTIVE_ACTIVE_CACHE);
        IgniteCache<Integer, Data> destCache = destCluster[0].getOrCreateCache(ACTIVE_ACTIVE_CACHE);

        runAsync(generateData(ACTIVE_ACTIVE_CACHE, srcCluster[srcCluster.length - 1],
            IntStream.range(0, KEYS_CNT).filter(i -> i % 2 == 0), 1));
        runAsync(generateData(ACTIVE_ACTIVE_CACHE, destCluster[destCluster.length - 1],
            IntStream.range(0, KEYS_CNT).filter(i -> i % 2 != 0), 1));

        IgniteInternalFuture<?> cdcSrcFut1 = igniteToKafka(srcCluster[0], SRC_DEST_TOPIC, ACTIVE_ACTIVE_CACHE);
        IgniteInternalFuture<?> cdcSrcFut2 = igniteToKafka(srcCluster[1], SRC_DEST_TOPIC, ACTIVE_ACTIVE_CACHE);
        IgniteInternalFuture<?> cdcDestFut1 = igniteToKafka(destCluster[0], DEST_SRC_TOPIC, ACTIVE_ACTIVE_CACHE);
        IgniteInternalFuture<?> cdcDestFut2 = igniteToKafka(destCluster[1], DEST_SRC_TOPIC, ACTIVE_ACTIVE_CACHE);

        try {
            KafkaToIgniteCdcStreamerConfiguration cfg1 = new KafkaToIgniteCdcStreamerConfiguration();
            KafkaToIgniteCdcStreamerConfiguration cfg2 = new KafkaToIgniteCdcStreamerConfiguration();

            cfg1.setTopic(SRC_DEST_TOPIC);
            cfg1.setCacheNames(Collections.singletonList(ACTIVE_ACTIVE_CACHE));

            cfg2.setTopic(DEST_SRC_TOPIC);
            cfg2.setCacheNames(Collections.singletonList(ACTIVE_ACTIVE_CACHE));

            IgniteInternalFuture<?> k2iFut1 = runAsync(new KafkaToIgniteCdcStreamer(destClusterCliCfg, props, cfg1));
            IgniteInternalFuture<?> k2iFut2 = runAsync(new KafkaToIgniteCdcStreamer(srcClusterCliCfg, props, cfg2));

            try {
                waitForSameData(srcCache, destCache, KEYS_CNT, BOTH_EXISTS, 1,
                    cdcDestFut1, cdcDestFut2, cdcDestFut1, cdcDestFut2, k2iFut1, k2iFut2);

                for (int i = 0; i < KEYS_CNT; i++) {
                    srcCache.put(i, generateSingleData(2));
                    destCache.put(i, generateSingleData(2));
                }

                waitForSameData(srcCache, destCache, KEYS_CNT, ANY_SAME_STATE, 2,
                    cdcDestFut1, cdcDestFut2, cdcDestFut1, cdcDestFut2, k2iFut1, k2iFut2);
            }
            finally {
                k2iFut1.cancel();
                k2iFut2.cancel();
            }
        }
        finally {
            cdcSrcFut1.cancel();
            cdcSrcFut2.cancel();
            cdcDestFut1.cancel();
            cdcDestFut2.cancel();
        }
    }

    /** */
    public void waitForSameData(
        IgniteCache<Integer, Data> src,
        IgniteCache<Integer, Data> dest,
        int keysCnt,
        int keysState,
        int iter,
        IgniteInternalFuture<?>...futs
    ) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() -> {
            for (int i = 0; i < keysCnt; i++) {
                if (keysState == BOTH_EXISTS) {
                    if (!src.containsKey(i) || !dest.containsKey(i))
                        return checkFuts(false, futs);
                }
                else if (keysState == BOTH_REMOVED) {
                    if (src.containsKey(i) || dest.containsKey(i))
                        return checkFuts(false, futs);

                    continue;
                }
                else if (keysState == ANY_SAME_STATE) {
                    if (src.containsKey(i) != dest.containsKey(i))
                        return checkFuts(false, futs);

                    if (!src.containsKey(i))
                        continue;
                }
                else
                    throw new IllegalArgumentException(keysState + " not supported.");

                Data data = dest.get(i);

                if (!data.equals(src.get(i)))
                    return checkFuts(false, futs);

                checkCRC(data, iter);
            }

            return checkFuts(true, futs);
        }, getTestTimeout()));
    }

    /** */
    private boolean checkFuts(boolean res, IgniteInternalFuture<?>...futs) {
        for (IgniteInternalFuture<?> fut : futs)
            assertFalse(fut.isDone());

        return res;
    }

    /** */
    public static void checkCRC(Data data, int iter) {
        assertEquals(iter, data.iter);

        crc.get().reset();
        crc.get().update(ByteBuffer.wrap(data.payload), data.payload.length);

        assertEquals(crc.get().getValue(), data.crc);
    }

    /**
     * @param ign Ignite instance to watch for.
     * @param topic Kafka topic name.
     * @param caches Cache name to stream to kafka.
     * @return Future for Change Data Capture application.
     */
    private IgniteInternalFuture<?> igniteToKafka(IgniteEx ign, String topic, String caches) {
        return runAsync(() -> {
            IgniteToKafkaCdcStreamer cdcCnsmr =
                new IgniteToKafkaCdcStreamer(topic, DFLT_PARTS, Collections.singleton(caches), KEYS_CNT, false, props);

            ChangeDataCaptureConfiguration cdcCfg = new ChangeDataCaptureConfiguration();

            cdcCfg.setConsumer(cdcCnsmr);

            new ChangeDataCapture(ign.configuration(), null, cdcCfg).run();
        });
    }

    /** */
    public static Runnable generateData(String cacheName, IgniteEx ign, IntStream keys, int iter) {
        return () -> {
            IgniteCache<Integer, Data> cache = ign.getOrCreateCache(cacheName);

            keys.forEach(i -> cache.put(i, generateSingleData(iter)));
        };
    }

    /**
     * @param iter Iteration number.
     * @return Generated data object.
     */
    public static Data generateSingleData(int iter) {
        crc.get().reset();

        byte[] payload = new byte[1024];

        ThreadLocalRandom.current().nextBytes(payload);

        crc.get().update(ByteBuffer.wrap(payload), 1024);

        return new Data(payload, crc.get().getValue(), iter, REQUEST_ID.incrementAndGet());
    }

    /** */
    public static class Data {
        /** */
        private final byte[] payload;

        /** */
        private final int crc;

        /** */
        private final int iter;

        /** */
        private final long reqId;

        /** */
        public Data(byte[] payload, int crc, int iter, long reqId) {
            this.payload = payload;
            this.crc = crc;
            this.iter = iter;
            this.reqId = reqId;
        }

        /** */
        public int getIter() {
            return iter;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Data data = (Data)o;
            return crc == data.crc && iter == data.iter && Arrays.equals(payload, data.payload);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = Objects.hash(crc, iter);
            result = 31 * result + Arrays.hashCode(payload);
            return result;
        }
    }

    /**
     * Create Kafka topic.
     *
     * @param topic Topic name
     * @param props Properties.
     */
    public static void createTopic(String topic, int kafkaParts, Properties props)
        throws InterruptedException, ExecutionException, TimeoutException {
        try (AdminClient adminCli = AdminClient.create(props)) {
            adminCli.createTopics(Collections.singleton(new NewTopic(
                topic,
                kafkaParts,
                (short)1
            ))).all().get(DFLT_REQ_TIMEOUT, TimeUnit.MINUTES);
        }
    }
}
