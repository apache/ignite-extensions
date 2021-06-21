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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public abstract class AbstractReplicationTest extends GridCommonAbstractTest {
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
    public static final String AP_CACHE = "active-passive-cache";

    /** */
    public static final String ACTIVE_ACTIVE_CACHE = "active-active-cache";

    /** */
    public static final byte SRC_CLUSTER_ID = 26;

    /** */
    public static final byte DEST_CLUSTER_ID = 27;

    /** */
    public static final int EXISTS = 1;

    /** */
    public static final int REMOVED = 2;

    /** */
    public static final int KEYS_CNT = 50;

    /** */
    protected static IgniteEx[] srcCluster;

    /** */
    protected static IgniteConfiguration[] srcClusterCliCfg;

    /** */
    protected static IgniteEx[] destCluster;

    /** */
    protected static IgniteConfiguration[] destClusterCliCfg;

    /** */
    private int commPort = TcpCommunicationSpi.DFLT_PORT;

    /** */
    private int discoPort = TcpDiscoverySpi.DFLT_PORT;

    /** */
    private byte clusterId = SRC_CLUSTER_ID;

    /** */
    protected int clientsCnt = 1;

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

        srcCluster = new IgniteEx[] {
            startGrid(1),
            startGrid(2)
        };

        srcClusterCliCfg = new IgniteConfiguration[clientsCnt];

        for (int i = 0; i < clientsCnt; i++)
            srcClusterCliCfg[i] = optimize(getConfiguration("src-cluster-client" + i).setClientMode(true));

        srcCluster[0].cluster().state(ACTIVE);
        srcCluster[0].cluster().tag("source");

        discoPort += DFLT_PORT_RANGE + 1;
        commPort += DFLT_PORT_RANGE + 1;
        clusterId = DEST_CLUSTER_ID;

        destCluster = new IgniteEx[] {
            startGrid(4),
            startGrid(5)
        };

        destClusterCliCfg = new IgniteConfiguration[clientsCnt];

        for (int i = 0; i < clientsCnt; i++)
            destClusterCliCfg[i] = optimize(getConfiguration("dest-cluster-client" + i).setClientMode(true));

        assertFalse("source".equals(destCluster[0].cluster().tag()));

        destCluster[0].cluster().state(ACTIVE);
        destCluster[0].cluster().tag("destination");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testActivePassiveReplication() throws Exception {
        List<IgniteInternalFuture<?>> futs = startActivePassiveCdc();

        try {
            IgniteCache<Integer, ConflictResolvableTestData> destCache = destCluster[0].createCache(AP_CACHE);

            destCache.put(1, ConflictResolvableTestData.create());
            destCache.remove(1);

            // Updates for "ignored-cache" should be ignored because of CDC consume configuration.
            runAsync(generateData("ignored-cache", srcCluster[srcCluster.length - 1], IntStream.range(0, KEYS_CNT)));
            runAsync(generateData(AP_CACHE, srcCluster[srcCluster.length - 1], IntStream.range(0, KEYS_CNT)));

            IgniteInternalFuture<?> k2iFut = startActivePassiveReplication();

            if (k2iFut != null)
                futs.add(k2iFut);

            IgniteCache<Integer, ConflictResolvableTestData> srcCache = srcCluster[srcCluster.length - 1].getOrCreateCache(AP_CACHE);

            waitForSameData(srcCache, destCache, KEYS_CNT, EXISTS, futs);

            IntStream.range(0, KEYS_CNT).forEach(srcCache::remove);

            waitForSameData(srcCache, destCache, KEYS_CNT, REMOVED, futs);
        }
        finally {
            for (IgniteInternalFuture<?> fut : futs)
                fut.cancel();
        }
    }

    /** */
    @Test
    public void testActiveActiveReplication() throws Exception {
        IgniteCache<Integer, ConflictResolvableTestData> srcCache = srcCluster[0].getOrCreateCache(ACTIVE_ACTIVE_CACHE);
        IgniteCache<Integer, ConflictResolvableTestData> destCache = destCluster[0].getOrCreateCache(ACTIVE_ACTIVE_CACHE);

        runAsync(generateData(ACTIVE_ACTIVE_CACHE, srcCluster[srcCluster.length - 1],
            IntStream.range(0, KEYS_CNT).filter(i -> i % 2 == 0)));
        runAsync(generateData(ACTIVE_ACTIVE_CACHE, destCluster[destCluster.length - 1],
            IntStream.range(0, KEYS_CNT).filter(i -> i % 2 != 0)));

        List<IgniteInternalFuture<?>> futs = startActiveActiveCdc();

        try {
            List<IgniteInternalFuture<?>> replicationFuts = startActiveActiveReplication();

            if (replicationFuts != null)
                futs.addAll(replicationFuts);

            waitForSameData(srcCache, destCache, KEYS_CNT, EXISTS, futs);

            runAsync(() -> IntStream.range(0, KEYS_CNT).filter(j -> j % 2 == 0).forEach(srcCache::remove));
            runAsync(() -> IntStream.range(0, KEYS_CNT).filter(j -> j % 2 != 0).forEach(destCache::remove));

            waitForSameData(srcCache, destCache, KEYS_CNT, REMOVED, futs);
        }
        finally {
            for (IgniteInternalFuture<?> fut : futs)
                fut.cancel();
        }
    }

    /** */
    public static Runnable generateData(String cacheName, IgniteEx ign, IntStream keys) {
        return () -> {
            IgniteCache<Integer, ConflictResolvableTestData> cache = ign.getOrCreateCache(cacheName);

            keys.forEach(i -> cache.put(i, ConflictResolvableTestData.create()));
        };
    }

    /** */
    public void waitForSameData(
        IgniteCache<Integer, ConflictResolvableTestData> src,
        IgniteCache<Integer, ConflictResolvableTestData> dest,
        int keysCnt,
        int keysState,
        List<IgniteInternalFuture<?>> futs
    ) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() -> {
            for (int i = 0; i < keysCnt; i++) {
                if (keysState == EXISTS) {
                    if (!src.containsKey(i) || !dest.containsKey(i))
                        return checkFuts(false, futs);
                }
                else if (keysState == REMOVED) {
                    if (src.containsKey(i) || dest.containsKey(i))
                        return checkFuts(false, futs);

                    continue;
                }
                else
                    throw new IllegalArgumentException(keysState + " not supported.");

                ConflictResolvableTestData data = dest.get(i);

                if (!data.equals(src.get(i)))
                    return checkFuts(false, futs);
            }

            return checkFuts(true, futs);
        }, getTestTimeout()));
    }

    /** */
    private boolean checkFuts(boolean res, List<IgniteInternalFuture<?>> futs) {
        for (IgniteInternalFuture<?> fut : futs)
            assertFalse(fut.isDone());

        return res;
    }

    /** */
    protected abstract List<IgniteInternalFuture<?>> startActivePassiveCdc();

    /** */
    protected abstract List<IgniteInternalFuture<?>> startActiveActiveCdc();

    /** */
    protected IgniteInternalFuture<?> startActivePassiveReplication() {
        return null;
    }

    /** */
    protected List<IgniteInternalFuture<?>> startActiveActiveReplication() {
        return null;
    }
}
