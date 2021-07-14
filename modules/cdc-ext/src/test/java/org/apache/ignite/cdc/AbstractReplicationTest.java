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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
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
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public abstract class AbstractReplicationTest extends GridCommonAbstractTest {
    /** Cache atomicity mode. */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicity;

    /** Cache replication mode. */
    @Parameterized.Parameter(1)
    public CacheMode mode;

    /** */
    @Parameterized.Parameter(2)
    public int backups;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "atomicity={0}, mode={1}, backupCnt={2}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode atomicity : EnumSet.of(ATOMIC, TRANSACTIONAL)) {
            for (CacheMode mode : EnumSet.of(PARTITIONED, REPLICATED)) {
                for (int backups = 0; backups < 2; backups++) {
                    // backupCount ignored for REPLICATED caches.
                    if (backups > 0 && mode == REPLICATED)
                        continue;

                    params.add(new Object[] {atomicity, mode, backups});
                }
            }
        }

        return params;
    }

    /** */
    public static final String ACTIVE_PASSIVE_CACHE = "active-passive-cache";

    /** */
    public static final String ACTIVE_ACTIVE_CACHE = "active-active-cache";

    /** */
    public static final String IGNORED_CACHE = "ignored-cache";

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
    public static final int KEYS_CNT = 500;

    /** */
    protected static IgniteEx[] srcCluster;

    /** */
    protected static IgniteEx[] destCluster;

    /** */
    protected static IgniteConfiguration[] srcClusterCliCfg;

    /** */
    protected static IgniteConfiguration[] destClusterCliCfg;

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
            CacheVersionConflictResolverPluginProvider<?> cfgPlugin1 = new CacheVersionConflictResolverPluginProvider<>();

            cfgPlugin1.setClusterId(clusterId);
            cfgPlugin1.setCaches(new HashSet<>(Arrays.asList(ACTIVE_PASSIVE_CACHE, ACTIVE_ACTIVE_CACHE)));
            cfgPlugin1.setConflictResolveField("reqId");

            CacheVersionConflictResolverPluginProvider<?> cfgPlugin2 = new CacheVersionConflictResolverPluginProvider<>();

            cfgPlugin2.setClusterId(clusterId);
            cfgPlugin2.setCaches(new HashSet<>(Arrays.asList("T1")));
            cfgPlugin2.setConflictResolveField("ID");

            cfg.setPluginProviders(cfgPlugin1);

            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

            cfg.getDataStorageConfiguration()
                .setWalForceArchiveTimeout(5_000)
                .setCdcEnabled(true);

            cfg.setConsistentId(igniteInstanceName);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        IgniteBiTuple<IgniteEx[], IgniteConfiguration[]> cluster = setupCluster("source", "src-cluster-client", 0);

        srcCluster = cluster.get1();
        srcClusterCliCfg = cluster.get2();

        discoPort += DFLT_PORT_RANGE + 1;
        clusterId = DEST_CLUSTER_ID;

        cluster = setupCluster("destination", "dest-cluster-client", 2);

        destCluster = cluster.get1();
        destClusterCliCfg = cluster.get2();

        String srcTag = srcCluster[0].cluster().tag();
        String destTag = destCluster[0].cluster().tag();

        assertNotNull(srcTag);
        assertNotNull(destTag);
        assertFalse(srcTag.equals(destTag));
    }

    /** */
    private IgniteBiTuple<IgniteEx[], IgniteConfiguration[]> setupCluster(
        String clusterTag,
        String clientPrefix,
        int idx
    ) throws Exception {
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
        List<IgniteInternalFuture<?>> futs = startActivePassiveCdc(ACTIVE_PASSIVE_CACHE);

        try {
            IgniteCache<Integer, ConflictResolvableTestData> destCache = createCache(destCluster[0], ACTIVE_PASSIVE_CACHE);

            destCache.put(KEYS_CNT + 1, ConflictResolvableTestData.create());
            destCache.remove(KEYS_CNT + 1);

            // Updates for "ignored-cache" should be ignored because of CDC consume configuration.
            runAsync(generateData(IGNORED_CACHE, srcCluster[srcCluster.length - 1], IntStream.range(0, KEYS_CNT)));
            runAsync(generateData(ACTIVE_PASSIVE_CACHE, srcCluster[srcCluster.length - 1], IntStream.range(0, KEYS_CNT)));

            IgniteCache<Integer, ConflictResolvableTestData> srcCache =
                createCache(srcCluster[srcCluster.length - 1], ACTIVE_PASSIVE_CACHE);

            waitForSameData(srcCache, destCache, KEYS_CNT, WaitDataMode.EXISTS, futs);

            IntStream.range(0, KEYS_CNT).forEach(srcCache::remove);

            waitForSameData(srcCache, destCache, KEYS_CNT, WaitDataMode.REMOVED, futs);

            assertFalse(destCluster[0].cacheNames().contains(IGNORED_CACHE));
        }
        finally {
            for (IgniteInternalFuture<?> fut : futs)
                fut.cancel();
        }
    }

    /** Active/Passive mode means changes made only in one cluster. */
    @Test
    public void testActivePassiveSqlDataReplication() throws Exception {
        String createTbl = "CREATE TABLE T1(ID BIGINT PRIMARY KEY, NAME VARCHAR) WITH \"CACHE_NAME=T1,VALUE_TYPE=T1Type\"";
        String insertQry = "INSERT INTO T1 VALUES(?, ?)";
        String deleteQry = "DELETE FROM T1";

        executeSql(srcCluster[0], createTbl);
        executeSql(destCluster[0], createTbl);

        executeSql(destCluster[0], insertQry, -1, "Name-1");
        executeSql(destCluster[0], deleteQry);

        IntStream.range(0, KEYS_CNT).forEach(id -> executeSql(srcCluster[0], insertQry, id, "Name" + id));

        List<IgniteInternalFuture<?>> futs = startActivePassiveCdc("T1");

        try {
            Function<Integer, GridAbsPredicate> waitForTblSz = expSz -> () -> {
                long cnt = (Long)executeSql(destCluster[0], "SELECT COUNT(*) FROM T1").get(0).get(0);

                return cnt == expSz;
            };

            assertTrue(waitForCondition(waitForTblSz.apply(KEYS_CNT), getTestTimeout()));


            List<List<?>> data = executeSql(destCluster[0], "SELECT ID, NAME FROM T1 ORDER BY ID");

            for (int i = 0; i < KEYS_CNT; i++) {
                assertEquals((long)i, data.get(i).get(0));
                assertEquals("Name" + i, data.get(i).get(1));
            }

            executeSql(srcCluster[0], deleteQry);

            assertTrue(waitForCondition(waitForTblSz.apply(0), getTestTimeout()));
        }
        finally {
            for (IgniteInternalFuture<?> fut : futs)
                fut.cancel();
        }
    }

    /** Active/Active mode means changes made in both clusters. */
    @Test
    public void testActiveActiveReplication() throws Exception {
        IgniteCache<Integer, ConflictResolvableTestData> srcCache = createCache(srcCluster[0], ACTIVE_ACTIVE_CACHE);
        IgniteCache<Integer, ConflictResolvableTestData> destCache = createCache(destCluster[0], ACTIVE_ACTIVE_CACHE);

        // Even keys goes to src cluster.
        runAsync(generateData(ACTIVE_ACTIVE_CACHE, srcCluster[srcCluster.length - 1],
            IntStream.range(0, KEYS_CNT).filter(i -> i % 2 == 0)));

        // Odd keys goes to dest cluster.
        runAsync(generateData(ACTIVE_ACTIVE_CACHE, destCluster[destCluster.length - 1],
            IntStream.range(0, KEYS_CNT).filter(i -> i % 2 != 0)));

        List<IgniteInternalFuture<?>> futs = startActiveActiveCdc();

        try {
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
    public Runnable generateData(String cacheName, IgniteEx ign, IntStream keys) {
        return () -> {
            IgniteCache<Integer, ConflictResolvableTestData> cache = createCache(ign, cacheName);

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

        return res;
    }

    /** */
    private IgniteCache<Integer, ConflictResolvableTestData> createCache(IgniteEx ignite, String name) {
        CacheConfiguration<Integer, ConflictResolvableTestData> ccfg = new CacheConfiguration<Integer, ConflictResolvableTestData>()
            .setName(name)
            .setCacheMode(mode)
            .setAtomicityMode(atomicity);

        if (mode != REPLICATED)
            ccfg.setBackups(backups);

        return ignite.getOrCreateCache(ccfg);
    }

    /** */
    private List<List<?>> executeSql(IgniteEx node, String sqlText, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(sqlText).setArgs(args), true).getAll();
    }

    /** */
    protected abstract List<IgniteInternalFuture<?>> startActivePassiveCdc(String cache);

    /** */
    protected abstract List<IgniteInternalFuture<?>> startActiveActiveCdc();
}
