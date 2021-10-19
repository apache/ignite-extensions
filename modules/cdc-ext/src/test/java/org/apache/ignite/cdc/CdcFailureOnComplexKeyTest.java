package org.apache.ignite.cdc;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class CdcFailureOnComplexKeyTest extends GridCommonAbstractTest {
    /** Test cache. */
    private static final String TEST_CACHE = "TEST_CACHE";

    /** Wal archive interval. */
    private static final int WAL_FORCE_ARCHIVE_TIMEOUT = 5_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDiscoverySpi(discoverySpi(igniteInstanceName));

        // DC0: id = 0, DC1: id = 1
        byte clusterId = (byte)(getTestIgniteInstanceIndex(igniteInstanceName));

        CacheVersionConflictResolverPluginProvider<?> cfgPlugin = new CacheVersionConflictResolverPluginProvider<>();
        cfgPlugin.setClusterId(clusterId);
        cfgPlugin.setCaches(new HashSet<>(Collections.singleton(TEST_CACHE)));

        cfg.setPluginProviders(cfgPlugin);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            .setWalForceArchiveTimeout(WAL_FORCE_ARCHIVE_TIMEOUT)
            .setCdcEnabled(true);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** */
    private TcpDiscoverySpi discoverySpi(String igniteInstanceName) {
        int discoPort = DFLT_PORT;

        boolean isDc1 = getTestIgniteInstanceIndex(igniteInstanceName) == 1;

        if (isDc1)
            discoPort = DFLT_PORT + DFLT_PORT_RANGE + 1;

        return new TcpDiscoverySpi()
            .setLocalPort(discoPort)
            .setIpFinder(
                new TcpDiscoveryVmIpFinder()
                    .setAddresses(Collections.singleton("127.0.0.1:" + discoPort + ".." + (discoPort + DFLT_PORT_RANGE))));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testFailureWithSql_notNullFields() throws Exception {
        doTest(cacheCreateAndFillViaSqlClosure(true));
    }

    /** */
    @Test
    public void testFailureWithSql_nullableFields() throws Exception {
        doTest(cacheCreateAndFillViaSqlClosure(false));
    }

    /** */
    @Test
    public void testFailureWithQueryEntity_noIndexInKey() throws Exception {
        doTest(cacheCreateAndFillWithQueryEntityClosure(false));
    }

    /** */
    @Test
    public void testFailureWithQueryEntity_extraIndexInKey() throws Exception {
        doTest(cacheCreateAndFillWithQueryEntityClosure(true));
    }

    /** */
    private void doTest(Runnable cacheCreateAndFillClo) throws Exception {
        // Ignite #0 - DC0
        IgniteEx ignite0 = startGrid(0);
        ignite0.cluster().state(ClusterState.ACTIVE);

        // Ignite #1 - DC1
        IgniteEx ignite1 = startGrid(1);
        ignite1.cluster().state(ClusterState.ACTIVE);

        // DC0 -> DC1 Ignite2Ignite CDC Streamer
        CdcMain cdc0 = cdc(ignite0.configuration(), ignite1.configuration(), "dc0-streamer");
        IgniteInternalFuture<?> fut0 = runAsync(cdc0);
        waitForRemoteNodes(ignite1, 1);

        cacheCreateAndFillClo.run();

        boolean cacheCreatedInDc0 = waitForCondition(() -> ignite0.cache(TEST_CACHE) != null, 5000);
        assertTrue("Cache was not created in DC0", cacheCreatedInDc0);

        boolean cacheCreatedInDc1 = waitForCondition(() -> ignite1.cache(TEST_CACHE) != null, 5000);
        assertTrue("Cache was not created in DC1", cacheCreatedInDc1);

        boolean itemsAddedToDc0 = waitForCondition(
            () -> ignite0.cache(TEST_CACHE).size(CachePeekMode.PRIMARY) == 2, 5000);

        assertTrue("DC0 was not updated", itemsAddedToDc0);

        log.info(">>>>>> Test Cache filled, wait WAL_FORCE_ARCHIVE_TIMEOUT");
        doSleep(WAL_FORCE_ARCHIVE_TIMEOUT);

        cdc0.stop();

        fut0.get();

        boolean itemsAddedToDc1 = waitForCondition(
            () -> ignite1.cache(TEST_CACHE).size(CachePeekMode.PRIMARY) == 2, 5000);

        assertTrue("DC1 was not updated", itemsAddedToDc1);

        stopAllGrids();
    }

    /** */
    private CdcMain cdc(
        IgniteConfiguration srcCfg,
        IgniteConfiguration dstCfg,
        String igniteInstanceName
    ) {
        IgniteConfiguration consumerCfg = new IgniteConfiguration()
            .setDiscoverySpi(discoverySpi(dstCfg.getIgniteInstanceName()))
            .setClientMode(true)
            .setIgniteInstanceName(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setPeerClassLoadingEnabled(true);

        CdcConsumer consumer = new IgniteToIgniteCdcStreamer(
            consumerCfg,
            false,
            new HashSet<>(Collections.singleton(TEST_CACHE)),
            256);

        CdcConfiguration cdcCfg = new CdcConfiguration();
        cdcCfg.setConsumer(consumer);

        return new CdcMain(srcCfg, null, cdcCfg);
    }

    /** */
    private Runnable cacheCreateAndFillViaSqlClosure(boolean isNull) {
        return () -> {
            final String CREATE_CACHE_SQL =
                "CREATE TABLE IF NOT EXISTS TEST (\n" +
                    "            ID INT %s,\n" +
                    "            SUBID VARCHAR %s,\n" +
                    "            NAME VARCHAR,\n" +
                    "            ORGID INT,\n" +
                    "            PRIMARY KEY (ID, SUBID))" +
                    "        WITH \"CACHE_NAME=" + TEST_CACHE + "," +
                    "KEY_TYPE=" + TestKey.class.getName() + "," +
                    "VALUE_TYPE=" + TestVal.class.getName() + "," +
                    "ATOMICITY=TRANSACTIONAL\";";

            try (IgniteClient clientDc0 = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"));
                 IgniteClient clientDc1 = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10801"))) {
                String notNullStr = isNull ? "NOT NULL" : "";

                String qry = String.format(CREATE_CACHE_SQL, notNullStr, notNullStr);

                clientDc0.query(new SqlFieldsQuery(qry)).getAll();

                clientDc1.query(new SqlFieldsQuery(qry)).getAll();

                ClientCache<TestKey, TestVal> cache0 = clientDc0.cache(TEST_CACHE);

                cache0.put(new TestKey(1, UUID.randomUUID().toString()), new TestVal("1", 10));
                cache0.put(new TestKey(2, UUID.randomUUID().toString()), new TestVal("2", 10));
            }
            catch (Exception e) {
                log.error("Client can't connect", e);
                fail("Exception occurred");
            }
        };
    }

    /** */
    private Runnable cacheCreateAndFillWithQueryEntityClosure(boolean addIdxToQryEntity) {
        return () -> {
            LinkedHashMap<String, String> fields = new LinkedHashMap<>();

            fields.put("id", Integer.class.getCanonicalName());
            fields.put("subId", String.class.getCanonicalName());
            fields.put("name", String.class.getCanonicalName());
            fields.put("orgId", Integer.class.getCanonicalName());

            QueryEntity qryEntity = new QueryEntity()
                .setKeyFields(new LinkedHashSet<>(Arrays.asList("id", "subId")))
                .setFields(fields)
                .setKeyType(TestKey.class.getName())
                .setValueType(TestVal.class.getName());

            if (addIdxToQryEntity)
                qryEntity.setIndexes(Collections.singleton(new QueryIndex("subId")));

            CacheConfiguration<TestKey, TestVal> ccfg =
                new CacheConfiguration<TestKey, TestVal>()
                    .setName(TEST_CACHE)
                    .setAtomicityMode(TRANSACTIONAL)
                    .setQueryEntities(Collections.singleton(qryEntity));

            IgniteCache<TestKey, TestVal> cache0 = grid(0).getOrCreateCache(ccfg);
            grid(1).getOrCreateCache(ccfg);

            cache0.put(new TestKey(10, "10"), new TestVal("name", 9));
            cache0.put(new TestKey(20, "11"), new TestVal("name1", 19));
        };
    }

    /** */
    private static class TestKey {
        /** Id. */
        private final int id;

        /** Sub id. */
        private final String subId;

        /** */
        public TestKey(int id, String subId) {
            this.id = id;
            this.subId = subId;
        }

        public int getId() {
            return id;
        }
    }

    /** */
    private static class TestVal {
        /** Name. */
        private final String name;

        /** Org id. */
        private final int orgId;

        /** */
        public TestVal(String name, int orgId) {
            this.name = name;
            this.orgId = orgId;
        }
    }
}
