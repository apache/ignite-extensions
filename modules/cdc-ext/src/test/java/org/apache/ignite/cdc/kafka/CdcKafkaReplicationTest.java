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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
        List<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode mode : EnumSet.of(ATOMIC, TRANSACTIONAL))
            for (int i = 0; i < 2; i++)
                params.add(new Object[] {mode, i});

        return params;
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
    protected static Properties props;

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setLocalPort(discoPort)
                .setIpFinder(new TcpDiscoveryVmIpFinder() {{
                    setAddresses(Collections.singleton("127.0.0.1:" + discoPort + ".." + (discoPort + DFLT_PORT_RANGE)));
                }}))
            .setCommunicationSpi(new TcpCommunicationSpi()
                .setLocalPort(commPort));

        if (!cfg.isClientMode()) {
            CacheVersionConflictResolverPluginProvider<?> cfgPlugin = new CacheVersionConflictResolverPluginProvider<>();

            cfgPlugin.setClusterId(clusterId);
            cfgPlugin.setCaches(new HashSet<>(Collections.singletonList(ACTIVE_ACTIVE_CACHE)));
            cfgPlugin.setConflictResolveField("reqId");

            cfg.setPluginProviders(cfgPlugin);

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
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
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
        IgniteInternalFuture<?> fut1 = igniteToKafka(srcCluster[0].configuration(), DFLT_TOPIC, AP_CACHE);
        IgniteInternalFuture<?> fut2 = igniteToKafka(srcCluster[1].configuration(), DFLT_TOPIC, AP_CACHE);

        try {
            IgniteCache<Integer, Data> destCache = destCluster[0].createCache(AP_CACHE);

            destCache.put(1, Data.create());
            destCache.remove(1);

            runAsync(generateData("cache-1", srcCluster[srcCluster.length - 1], IntStream.range(0, KEYS_CNT)));
            runAsync(generateData(AP_CACHE, srcCluster[srcCluster.length - 1], IntStream.range(0, KEYS_CNT)));

            IgniteInternalFuture<?> k2iFut = kafkaToIgnite(AP_CACHE, DFLT_TOPIC, destClusterCliCfg);

            try {
                IgniteCache<Integer, Data> srcCache = srcCluster[srcCluster.length - 1].getOrCreateCache(AP_CACHE);

                waitForSameData(srcCache, destCache, KEYS_CNT, BOTH_EXISTS, fut1, fut2, k2iFut);

                IntStream.range(0, KEYS_CNT).forEach(srcCache::remove);

                waitForSameData(srcCache, destCache, KEYS_CNT, BOTH_REMOVED, fut1, fut2, k2iFut);
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
            IntStream.range(0, KEYS_CNT).filter(i -> i % 2 == 0)));
        runAsync(generateData(ACTIVE_ACTIVE_CACHE, destCluster[destCluster.length - 1],
            IntStream.range(0, KEYS_CNT).filter(i -> i % 2 != 0)));

        IgniteInternalFuture<?> cdcSrcFut1 = igniteToKafka(srcCluster[0].configuration(), SRC_DEST_TOPIC, ACTIVE_ACTIVE_CACHE);
        IgniteInternalFuture<?> cdcSrcFut2 = igniteToKafka(srcCluster[1].configuration(), SRC_DEST_TOPIC, ACTIVE_ACTIVE_CACHE);
        IgniteInternalFuture<?> cdcDestFut1 = igniteToKafka(destCluster[0].configuration(), DEST_SRC_TOPIC, ACTIVE_ACTIVE_CACHE);
        IgniteInternalFuture<?> cdcDestFut2 = igniteToKafka(destCluster[1].configuration(), DEST_SRC_TOPIC, ACTIVE_ACTIVE_CACHE);

        try {
            IgniteInternalFuture<?> k2iFut1 = kafkaToIgnite(ACTIVE_ACTIVE_CACHE, SRC_DEST_TOPIC, destClusterCliCfg);
            IgniteInternalFuture<?> k2iFut2 = kafkaToIgnite(ACTIVE_ACTIVE_CACHE, DEST_SRC_TOPIC, srcClusterCliCfg);

            try {
                waitForSameData(srcCache, destCache, KEYS_CNT, BOTH_EXISTS,
                    cdcDestFut1, cdcDestFut2, cdcDestFut1, cdcDestFut2, k2iFut1, k2iFut2);

                for (int i = 0; i < KEYS_CNT; i++) {
                    srcCache.put(i, Data.create());
                    destCache.put(i, Data.create());
                }

                waitForSameData(srcCache, destCache, KEYS_CNT, ANY_SAME_STATE,
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

    /**
     * @param igniteCfg Ignite configuration.
     * @param topic Kafka topic name.
     * @param caches Cache name to stream to kafka.
     * @return Future for Change Data Capture application.
     */
    protected IgniteInternalFuture<?> igniteToKafka(IgniteConfiguration igniteCfg, String topic, String caches) {
        return runAsync(() -> {
            IgniteToKafkaCdcStreamer cdcCnsmr =
                new IgniteToKafkaCdcStreamer(topic, DFLT_PARTS, Collections.singleton(caches), KEYS_CNT, false, props);

            ChangeDataCaptureConfiguration cdcCfg = new ChangeDataCaptureConfiguration();

            cdcCfg.setConsumer(cdcCnsmr);

            new ChangeDataCapture(igniteCfg, null, cdcCfg).run();
        });
    }

    /**
     * @param cacheName Cache name.
     * @param igniteCfg Ignite configuration.
     * @return Future for runed {@link KafkaToIgniteCdcStreamer}.
     */
    protected IgniteInternalFuture<?> kafkaToIgnite(String cacheName, String topic, IgniteConfiguration igniteCfg) {
        KafkaToIgniteCdcStreamerConfiguration cfg = new KafkaToIgniteCdcStreamerConfiguration();

        cfg.setCaches(Collections.singletonList(cacheName));
        cfg.setTopic(topic);

        return runAsync(new KafkaToIgniteCdcStreamer(igniteCfg, props, cfg));
    }

    /** */
    public static Runnable generateData(String cacheName, IgniteEx ign, IntStream keys) {
        return () -> {
            IgniteCache<Integer, Data> cache = ign.getOrCreateCache(cacheName);

            keys.forEach(i -> cache.put(i, Data.create()));
        };
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
