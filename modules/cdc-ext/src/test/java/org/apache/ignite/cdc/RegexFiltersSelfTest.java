package org.apache.ignite.cdc;

import java.util.Collections;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cdc.thin.IgniteToIgniteClientCdcStreamer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class RegexFiltersSelfTest extends GridCommonAbstractTest {

    /** */
    private IgniteEx src;

    /** */
    private IgniteEx dest;

    /** */
    private AbstractIgniteCdcStreamer streamer;

    /** */
    private int discoPort = TcpDiscoverySpi.DFLT_PORT;

    /** */
    private enum WaitDataMode {
        /** */
        EXISTS,

        /** */
        REMOVED
    }

    /** */
    private static final String TEST_CACHE = "test-cache";

    /** */
    private static final String REGEX_MATCHING_CACHE_1 = "regex-cache1";

    /** */
    private static final String REGEX_MATCHING_CACHE_2 = "regex-cache2";

    /** */
    private static final String REGEX_MATCHING_CACHE_3 = "my-cache1";

    /** */
    private static final String REGEX_EXCLUDED_CACHE = "forbidden-cache1";

    /** */
    private static final String REGEX_INCLUDE_PATTERN = "regex.*";

    /** */
    private static final String REGEX_EXCLUDE_PATTERN = "forbidden.*";

    /** */
    private static final int KEYS_CNT = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder()
            .setAddresses(Collections.singleton("127.0.0.1:" + discoPort + ".." + (discoPort + DFLT_PORT_RANGE)));

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setLocalPort(discoPort)
                .setIpFinder(finder));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setCdcEnabled(true)));

        cfg.getDataStorageConfiguration()
            .setWalForceArchiveTimeout(5_000);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName(REGEX_MATCHING_CACHE_2)
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(TRANSACTIONAL));

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /**
     *
     * @param srcCfg Ignite source node configuration.
     * @param cache Cache name to stream to Ignite2Ignite.
     * @param includeTemplate Include cache template.
     * @param excludeTemplate Exclude cache template.
     * @return Future for Change Data Capture application.
     */
    private IgniteInternalFuture<?> startCdc(
        IgniteConfiguration srcCfg,
        String cache,
        String includeTemplate,
        String excludeTemplate
    ) {
        return runAsync(() -> {
            CdcConfiguration cdcCfg = new CdcConfiguration();

             streamer = new TestI2IClientCdcStreamer()
                .setDestinationClientConfiguration(new ClientConfiguration()
                .setAddresses(F.first(dest.localNode().addresses()) + ":"
                    + dest.localNode().attribute(ClientListenerProcessor.CLIENT_LISTENER_PORT)));

            streamer.setMaxBatchSize(KEYS_CNT);
            streamer.setCaches(Collections.singleton(cache));
            streamer.setIncludeTemplate(includeTemplate);
            streamer.setExcludeTemplate(excludeTemplate);

            cdcCfg.setConsumer(streamer);
            cdcCfg.setMetricExporterSpi(new JmxMetricExporterSpi());

            CdcMain cdc = new CdcMain(srcCfg, null, cdcCfg);

            cdc.run();
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        src = startGrid(getConfiguration("source-cluster"));

        discoPort += DFLT_PORT_RANGE + 1;

        dest = startGrid(getConfiguration("dest-cluster"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    public void waitForSameData(
        IgniteCache<Integer, Integer> src,
        IgniteCache<Integer, Integer> dest,
        int keysCnt,
        WaitDataMode mode,
        IgniteInternalFuture<?> fut
    ) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(() -> {
            for (int i = 0; i < keysCnt; i++) {
                if (mode == WaitDataMode.EXISTS) {
                    if (!src.containsKey(i) || !dest.containsKey(i))
                        return checkFut(false, fut);
                }
                else if (mode == WaitDataMode.REMOVED) {
                    if (src.containsKey(i) || dest.containsKey(i))
                        return checkFut(false, fut);

                    continue;
                }
                else
                    throw new IllegalArgumentException(mode + " not supported.");

                Integer data = dest.get(i);

                if (!data.equals(src.get(i)))
                    return checkFut(false, fut);
            }

            return checkFut(true, fut);
        }, getTestTimeout()));
    }

    /** */
    private boolean checkFut(boolean res, IgniteInternalFuture<?> fut) {
        assertFalse("Fut error: " + X.getFullStackTrace(fut.error()), fut.isDone());

        return res;
    }

    /** */
    public Runnable generateData(IgniteCache<Integer, Integer> cache, IntStream keys) {
        return () -> {
            keys.forEach(i -> cache.put(i, i * 2));
        };
    }

    /** */
    public IgniteCache<Integer, Integer> getCache(IgniteEx cluster, String cacheName) {
        return cluster.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(cacheName)
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(TRANSACTIONAL));
    }

    /**
     * Test checks whether caches added by regex filters are returned to replication after CDC restart.
     */
    @Test
    public void testRegexFiltersOnCdcRestart() throws Exception {
        src.cluster().state(ClusterState.ACTIVE);

        dest.cluster().state(ClusterState.ACTIVE);

        //Start CDC only with 'test-cache' set in 'caches' property of streamer config and cache masks (regex filters)
        IgniteInternalFuture<?> cdc = startCdc(src.configuration(), TEST_CACHE, REGEX_INCLUDE_PATTERN, "");

        IgniteCache<Integer, Integer> srcCache = getCache(src, REGEX_MATCHING_CACHE_1);

        IgniteCache<Integer, Integer> destCache = getCache(dest, REGEX_MATCHING_CACHE_1);

        IgniteCache<Integer, Integer> srcCache2 = src.getOrCreateCache(REGEX_MATCHING_CACHE_2);

        IgniteCache<Integer, Integer> destCache2 = dest.getOrCreateCache(REGEX_MATCHING_CACHE_2);

        //Cache created through CacheConfiguration on cluster start should be also added to CDC
        try {
            runAsync(generateData(srcCache2, IntStream.range(0, KEYS_CNT)));

            waitForSameData(srcCache2, destCache2, KEYS_CNT, WaitDataMode.EXISTS, cdc);
        }
        finally {
            cdc.cancel();
        }

        //Restart CDC
        IgniteInternalFuture<?> cdc2 = startCdc(src.configuration(), TEST_CACHE, REGEX_INCLUDE_PATTERN, "");

        try {
            runAsync(generateData(srcCache, IntStream.range(0, KEYS_CNT)));

            waitForSameData(srcCache, destCache, KEYS_CNT, WaitDataMode.EXISTS, cdc2);
        }
        finally {
            cdc2.cancel();
        }
    }

    /**
     * Test checks whether after changing regex filters config, existing caches matching new criteria are added to CDC.
     */
    @Test
    public void testRegexConfigChange() throws Exception {
        src.cluster().state(ClusterState.ACTIVE);

        dest.cluster().state(ClusterState.ACTIVE);

        IgniteInternalFuture<?> cdc = startCdc(src.configuration(), TEST_CACHE, REGEX_INCLUDE_PATTERN, "");

        IgniteCache<Integer, Integer> srcCache = getCache(src, REGEX_MATCHING_CACHE_1);

        IgniteCache<Integer, Integer> destCache = getCache(dest, REGEX_MATCHING_CACHE_1);

        IgniteCache<Integer, Integer> srcCache2 = getCache(src, REGEX_MATCHING_CACHE_3);

        IgniteCache<Integer, Integer> destCache2 = getCache(dest, REGEX_MATCHING_CACHE_3);

        cdc.cancel();

        final String NEW_REGEX_PATTERN = REGEX_INCLUDE_PATTERN + "|my-cache.*";

        //Restart CDC with the new regexp config
        IgniteInternalFuture<?> cdc2 = startCdc(src.configuration(), TEST_CACHE, NEW_REGEX_PATTERN, " ");

        try {
            runAsync(generateData(srcCache, IntStream.range(0, KEYS_CNT)));

            runAsync(generateData(srcCache2, IntStream.range(0, KEYS_CNT)));

            waitForSameData(srcCache, destCache, KEYS_CNT, WaitDataMode.EXISTS, cdc2);

            waitForSameData(srcCache2, destCache2, KEYS_CNT, WaitDataMode.EXISTS, cdc2);
        }
        finally {
            cdc2.cancel();
        }
    }

    /**
     * Test checks whether caches matching exclude regex filters are excluded from CDC.
     */
    @Test
    public void testRegexExcludeFilters() throws Exception {
        src.cluster().state(ClusterState.ACTIVE);

        dest.cluster().state(ClusterState.ACTIVE);

        IgniteInternalFuture<?> cdc = startCdc(src.configuration(), TEST_CACHE, "", REGEX_EXCLUDE_PATTERN);

        IgniteCache<Integer, Integer> srcCache = getCache(src, REGEX_EXCLUDED_CACHE);

        runAsync(generateData(srcCache, IntStream.range(0, KEYS_CNT)));

        assertTrue(waitForCondition(() -> srcCache.size() == KEYS_CNT, getTestTimeout()));

        TestI2IClientCdcStreamer strmr = (TestI2IClientCdcStreamer) streamer;

        assertEquals(1, strmr.getCacheIds().size());

        assertTrue(strmr.getCacheIds().contains(TEST_CACHE.hashCode()));

        cdc.cancel();
    }

    /** */
    private static class TestI2IClientCdcStreamer extends IgniteToIgniteClientCdcStreamer {
        public Set<Integer> getCacheIds() {
            return cachesIds;
        }
    }
}
