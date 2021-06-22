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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
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
    public static final String ACTIVE_PASSIVE_CACHE = "active-passive-cache";

    /** */
    public static final String ACTIVE_ACTIVE_CACHE = "active-active-cache";

    /** */
    public static final byte SRC_CLUSTER_ID = 1;

    /** */
    public static final byte DEST_CLUSTER_ID = 2;

    /** */
    private enum WaitDataMode {
        /** */
        EXISTS,

        /** */
        REMOVED
    }

    /** */
    public static final int KEYS_CNT = 1000;

    /** */
    protected static IgniteBiTuple<IgniteEx[], IgniteConfiguration[]> srcCluster;

    /** */
    protected static IgniteBiTuple<IgniteEx[], IgniteConfiguration[]> destCluster;

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
                }}));

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

        srcCluster = setupCluster("source", "src-cluster-client", 0);

        discoPort += DFLT_PORT_RANGE + 1;
        clusterId = DEST_CLUSTER_ID;

        destCluster = setupCluster("destination", "dest-cluster-client", 2);
    }

    /** */
    private IgniteBiTuple<IgniteEx[], IgniteConfiguration[]> setupCluster(String clusterTag, String clientPrefix, int idx) throws Exception {
        IgniteEx[] cluster = new IgniteEx[] {
            startGrid(idx + 1),
            startGrid(idx + 2)
        };

        IgniteConfiguration[] clusterCliCfg = new IgniteConfiguration[2];

        for (int i = 0; i < 2; i++)
            clusterCliCfg[i] = optimize(getConfiguration(clientPrefix + i).setClientMode(true));

        cluster[0].cluster().state(ACTIVE);
        cluster[0].cluster().tag(clusterTag);

        return F.t(cluster, clusterCliCfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** Active/Passive mode means changes made only in one cluster. */
    @Test
    public void testActivePassiveReplication() throws Exception {
        List<IgniteInternalFuture<?>> futs = startActivePassiveCdc();

        try {
            IgniteCache<Integer, ConflictResolvableTestData> destCache = destCluster.get1()[0].createCache(ACTIVE_PASSIVE_CACHE);

            destCache.put(1, ConflictResolvableTestData.create());
            destCache.remove(1);

            // Updates for "ignored-cache" should be ignored because of CDC consume configuration.
            runAsync(generateData("ignored-cache", srcCluster.get1()[srcCluster.get1().length - 1], IntStream.range(0, KEYS_CNT)));
            runAsync(generateData(ACTIVE_PASSIVE_CACHE, srcCluster.get1()[srcCluster.get1().length - 1], IntStream.range(0, KEYS_CNT)));

            List<IgniteInternalFuture<?>> k2iFut = startActivePassiveReplication();

            if (k2iFut != null)
                futs.addAll(k2iFut);

            IgniteCache<Integer, ConflictResolvableTestData> srcCache =
                srcCluster.get1()[srcCluster.get1().length - 1].getOrCreateCache(ACTIVE_PASSIVE_CACHE);

            waitForSameData(srcCache, destCache, KEYS_CNT, WaitDataMode.EXISTS, futs);

            IntStream.range(0, KEYS_CNT).forEach(srcCache::remove);

            waitForSameData(srcCache, destCache, KEYS_CNT, WaitDataMode.REMOVED, futs);
        }
        finally {
            for (IgniteInternalFuture<?> fut : futs)
                fut.cancel();
        }
    }

    /** Active/Active mode means changes made in both clusters. */
    @Test
    public void testActiveActiveReplication() throws Exception {
        IgniteCache<Integer, ConflictResolvableTestData> srcCache = srcCluster.get1()[0].getOrCreateCache(ACTIVE_ACTIVE_CACHE);
        IgniteCache<Integer, ConflictResolvableTestData> destCache = destCluster.get1()[0].getOrCreateCache(ACTIVE_ACTIVE_CACHE);

        runAsync(generateData(ACTIVE_ACTIVE_CACHE, srcCluster.get1()[srcCluster.get1().length - 1],
            IntStream.range(0, KEYS_CNT).filter(i -> i % 2 == 0)));
        runAsync(generateData(ACTIVE_ACTIVE_CACHE, destCluster.get1()[destCluster.get1().length - 1],
            IntStream.range(0, KEYS_CNT).filter(i -> i % 2 != 0)));

        List<IgniteInternalFuture<?>> futs = startActiveActiveCdc();

        try {
            List<IgniteInternalFuture<?>> replicationFuts = startActiveActiveReplication();

            if (replicationFuts != null)
                futs.addAll(replicationFuts);

            waitForSameData(srcCache, destCache, KEYS_CNT, WaitDataMode.EXISTS, futs);

            runAsync(() -> IntStream.range(0, KEYS_CNT).filter(j -> j % 2 == 0).forEach(srcCache::remove));
            runAsync(() -> IntStream.range(0, KEYS_CNT).filter(j -> j % 2 != 0).forEach(destCache::remove));

            waitForSameData(srcCache, destCache, KEYS_CNT, WaitDataMode.REMOVED, futs);
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
        WaitDataMode mode,
        List<IgniteInternalFuture<?>> futs
    ) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() -> {
            for (int i = 0; i < keysCnt; i++) {
                if (mode == WaitDataMode.EXISTS) {
                    if (!src.containsKey(i) || !dest.containsKey(i))
                        return checkFuts(false, futs);
                }
                else if (mode == WaitDataMode.REMOVED) {
                    if (src.containsKey(i) || dest.containsKey(i))
                        return checkFuts(false, futs);

                    continue;
                }
                else
                    throw new IllegalArgumentException(mode + " not supported.");

                ConflictResolvableTestData data = dest.get(i);

                if (!data.equals(src.get(i)))
                    return checkFuts(false, futs);
            }

            return checkFuts(true, futs);
        }, getTestTimeout()));
    }

    /** */
    private boolean checkFuts(boolean res, List<IgniteInternalFuture<?>> futs) {
        for (int i = 0; i < futs.size(); i++)
            assertFalse("Fut " + i, futs.get(i).isDone());

        for (IgniteInternalFuture<?> fut : futs)
            assertFalse(fut.isDone());

        return res;
    }

    /** */
    protected abstract List<IgniteInternalFuture<?>> startActivePassiveCdc();

    /** */
    protected abstract List<IgniteInternalFuture<?>> startActiveActiveCdc();

    /** */
    protected List<IgniteInternalFuture<?>> startActivePassiveReplication() {
        return null;
    }

    /** */
    protected List<IgniteInternalFuture<?>> startActiveActiveReplication() {
        return null;
    }
}
