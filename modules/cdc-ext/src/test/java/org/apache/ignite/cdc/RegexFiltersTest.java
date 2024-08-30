package org.apache.ignite.cdc;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
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

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class RegexFiltersTest extends GridCommonAbstractTest {

    /** */
    private IgniteEx src;

    /** */
    private IgniteEx dest;

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
    private static final String REGEX_MATCHING_CACHE = "regex-cache";

    /** */
    private static final String REGEX_INCLUDE_PATTERN = "regex.*";

    /** */
    private Set<String> includeTemplates;

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

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /**
     *
     * @param srcCfg Ignite source node configuration.
     * @param cache Cache name to stream to Ignite2Ignite.
     * @param includeTemplates Include cache templates.
     * @param excludeTemplates Exclude cache templates.
     * @return Future for Change Data Capture application.
     */
    private IgniteInternalFuture<?> startCdc(IgniteConfiguration srcCfg,
                                             String cache,
                                             Set<String> includeTemplates,
                                             Set<String> excludeTemplates) {
        return runAsync(() -> {
            CdcConfiguration cdcCfg = new CdcConfiguration();

            AbstractIgniteCdcStreamer streamer = new IgniteToIgniteClientCdcStreamer()
                .setDestinationClientConfiguration(new ClientConfiguration()
                .setAddresses(F.first(dest.localNode().addresses()) + ":"
                    + dest.localNode().attribute(ClientListenerProcessor.CLIENT_LISTENER_PORT)));

            streamer.setMaxBatchSize(KEYS_CNT);
            streamer.setCaches(Collections.singleton(cache));
            streamer.setIncludeTemplates(includeTemplates);
            streamer.setExcludeTemplates(excludeTemplates);

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

        includeTemplates = new HashSet<>(Arrays.asList(REGEX_INCLUDE_PATTERN));
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

    /**
     * Test checks whether caches added by regex filters are saved to and read from file after CDC restart.
     */
    @Test
    public void testRegexFiltersOnCdcRestart() throws Exception {

        src.cluster().state(ClusterState.ACTIVE);

        dest.cluster().state(ClusterState.ACTIVE);

        //Start CDC only with 'test-cache' in config and cache masks (regex filters)
        IgniteInternalFuture<?> cdc = startCdc(src.configuration(), TEST_CACHE, includeTemplates, Collections.emptySet());

        IgniteCache<Integer, Integer> srcCache = src.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(REGEX_MATCHING_CACHE)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        IgniteCache<Integer, Integer> destCache = dest.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(REGEX_MATCHING_CACHE)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        cdc.cancel();

        //Restart CDC
        IgniteInternalFuture<?> cdc2 = startCdc(src.configuration(), TEST_CACHE, includeTemplates, Collections.emptySet());

        try {
            runAsync(generateData(srcCache, IntStream.range(0, KEYS_CNT)));

            waitForSameData(srcCache, destCache, KEYS_CNT, WaitDataMode.EXISTS, cdc2);
        }
        finally {
            cdc2.cancel();
        }
    }
}
