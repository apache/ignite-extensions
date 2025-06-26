package org.apache.ignite.cdc.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcConsumer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class CdcPostgreSqlReplicationTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter()
    public boolean onlyPrimary;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicity;

    /** */
    @Parameterized.Parameter(2)
    public CacheMode mode;

    /** */
    @Parameterized.Parameter(3)
    public int backups;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "onlyPrimary={0}, atomicity={1}, mode={2}, backupCnt={3}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (boolean onlyPrimary : new boolean[] {true, false}) {
            for (CacheAtomicityMode atomicity : EnumSet.of(ATOMIC, TRANSACTIONAL)) {
                for (CacheMode mode : EnumSet.of(PARTITIONED, REPLICATED)) {
                    for (int backups = 0; backups < 2; backups++) {
                        // backupCount ignored for REPLICATED caches.
                        if (backups > 0 && mode == REPLICATED)
                            continue;

                        params.add(new Object[] {onlyPrimary, atomicity, mode, backups});
                    }
                }
            }
        }

        return params;
    }

    /** */
    private static final int KEYS_CNT = 1024;

    /** */
    private static final int MAX_BATCH_SIZE = 128;

    /** */
    private static final int MAX_VALUE_SIZE = 32;

    /** */
    private static IgniteEx[] srcCluster;

    /** */
    private static EmbeddedPostgres postgres;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration dataRegionConfiguration = new DataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setCdcEnabled(true);

        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration()
            .setWalForceArchiveTimeout(5_000)
            .setDefaultDataRegionConfiguration(dataRegionConfiguration);

        cfg.setDataStorageConfiguration(dataStorageConfiguration);
        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        srcCluster = new IgniteEx[] {
            startGrid(0),
            startGrid(1)
        };

        srcCluster[0].cluster().state(ClusterState.ACTIVE);

        postgres = EmbeddedPostgres.builder().start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        assertTrue(hasLocalUpdates());

        stopAllGrids();

        cleanPersistenceDir();

        postgres.close();
    }

    /** */
    @Test
    public void testSingleColumnKeyDataReplication() throws Exception {
        String backupsStr = mode == PARTITIONED ? "BACKUPS=" + backups + "," : "";

        String createTbl = "CREATE TABLE T1(ID BIGINT PRIMARY KEY, NAME VARCHAR) WITH \"" +
            "CACHE_NAME=T1," +
            "VALUE_TYPE=T1Type," +
            "ATOMICITY=" + atomicity.name() + "," +
            backupsStr +
            "TEMPLATE=" + mode.name() + "\";";

        executeSql(srcCluster[0], createTbl);

        String insertQry = "INSERT INTO T1 VALUES(?, ?)";

        IntConsumer insert = id -> executeSql(srcCluster[0], insertQry, id, "Name" + id);
        Supplier<Boolean> checkInsert = () -> checkT1Table(cnt -> cnt, cnt -> "Name" + cnt);

        IntConsumer insertForUpdate = id -> executeSql(srcCluster[0], insertQry, 2 * id, id + "Name");
        Supplier<Boolean> checkInsertForUpdate = () -> checkT1Table(cnt -> 2 * cnt, cnt -> cnt + "Name");

        testDataReplication("T1", insert, checkInsert, insertForUpdate, checkInsertForUpdate);
    }

    /** */
    private boolean checkT1Table(Function<Long, Long> cntToId, Function<Long, String> cntToName) {
        try (ResultSet res = executeOnPostgreSql("SELECT ID, NAME FROM T1 ORDER BY ID")) {
            long cnt = 0;

            int id;
            String name;

            while (res.next()) {
                id = res.getInt("ID");
                name = res.getString("NAME");

                assertEquals((long)cntToId.apply(cnt), id);
                assertEquals(cntToName.apply(cnt), name);

                cnt++;
            }

            assert cnt == KEYS_CNT;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }

        return true;
    }

    /** Replication with complex SQL key. Data inserted via SQL. */
    @Test
    public void testMultiColumnKeyDataReplication() throws Exception {
        String backupsStr = mode == PARTITIONED ? "BACKUPS=" + backups + "," : "";

        String createTbl = "CREATE TABLE IF NOT EXISTS T2 (" +
            "    ID INT NOT NULL, " +
            "    SUBID VARCHAR NOT NULL, " +
            "    NAME VARCHAR, " +
            "    ORGID INT, " +
            "    PRIMARY KEY (ID, SUBID))" +
            "    WITH \"CACHE_NAME=T2," +
            "KEY_TYPE=" + TestKey.class.getName() + "," +
            "VALUE_TYPE=" + TestVal.class.getName() + "," +
            "ATOMICITY=" + atomicity.name() + "," +
            backupsStr +
            "TEMPLATE=" + mode.name() + "\";";

        executeSql(srcCluster[0], createTbl);

        IntConsumer insert = id -> executeSql(
            srcCluster[0],
            "INSERT INTO T2 (ID, SUBID, NAME, ORGID) VALUES(?, ?, ?, ?)",
            id,
            "SUBID",
            "Name" + id,
            id * 42
        );

        Supplier<Boolean> checkInsert = () -> checkT2T3Table("T2", cnt -> cnt, cnt -> "Name" + cnt);

        IntConsumer insertForUpdate = id -> executeSql(
            srcCluster[0],
            "INSERT INTO T2 (ID, SUBID, NAME, ORGID) VALUES(?, ?, ?, ?)",
            2 * id,
            "SUBID",
            id + "Name",
            id * 42
        );

        Supplier<Boolean> checkInsertForUpdate = () -> checkT2T3Table("T2", cnt -> 2 * cnt, cnt -> cnt + "Name");

        testDataReplication("T2", insert, checkInsert, insertForUpdate, checkInsertForUpdate);
    }

    /** Replication with complex SQL key. Data inserted via key-value API. */
    @Test
    public void testMultiColumnKeyDataReplicationWithKeyValue() throws Exception {
        String backupsStr = mode == PARTITIONED ? "BACKUPS=" + backups + "," : "";

        String createTbl = "CREATE TABLE IF NOT EXISTS T3 (" +
            "    ID INT NOT NULL, " +
            "    SUBID VARCHAR NOT NULL, " +
            "    NAME VARCHAR, " +
            "    ORGID INT, " +
            "    PRIMARY KEY (ID, SUBID))" +
            "    WITH \"CACHE_NAME=T3," +
            "KEY_TYPE=" + TestKey.class.getName() + "," +
            "VALUE_TYPE=" + TestVal.class.getName() + "," +
            "ATOMICITY=" + atomicity.name() + "," +
            backupsStr +
            "TEMPLATE=" + mode.name() + "\";";

        executeSql(srcCluster[0], createTbl);

        IntConsumer insert = id -> srcCluster[0].cache("T3")
            .put(
                new TestKey(id, "SUBID"),
                new TestVal("Name" + id, id * 42)
            );

        Supplier<Boolean> checkInsert = () -> checkT2T3Table("T3", cnt -> cnt, cnt -> "Name" + cnt);

        IntConsumer insertForUpdate = id -> srcCluster[0].cache("T3")
            .put(
                new TestKey(2 * id, "SUBID"),
                new TestVal(id + "Name", id * 42)
            );

        Supplier<Boolean> checkInsertForUpdate = () -> checkT2T3Table("T3", cnt -> 2 * cnt, cnt -> cnt + "Name");

        testDataReplication("T3", insert, checkInsert, insertForUpdate, checkInsertForUpdate);
    }

    /** */
    private boolean checkT2T3Table(String tableName, Function<Long, Long> cntToId, Function<Long, String> cntToName) {
        try (ResultSet res = executeOnPostgreSql("SELECT ID, SUBID, NAME, ORGID FROM " + tableName + " ORDER BY ID")) {
            long cnt = 0;

            int id;
            String subId;
            String name;
            int orgId;

            while (res.next()) {
                id = res.getInt("ID");
                subId = res.getString("SUBID");
                name = res.getString("NAME");
                orgId = res.getInt("ORGID");

                assertEquals((long)cntToId.apply(cnt), id);
                assertEquals("SUBID", subId);
                assertEquals(cntToName.apply(cnt), name);
                assertEquals(42 * cnt, orgId);

                cnt++;
            }

            assert cnt == KEYS_CNT;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }

        return true;
    }

    /** */
    private void testDataReplication(
        String tableName,
        IntConsumer insert,
        Supplier<Boolean> checkInsert,
        IntConsumer insertForUpdate,
        Supplier<Boolean> checkInsertForUpdate
    ) throws Exception {
        String deleteQry = "DELETE FROM " + tableName;

        List<IgniteInternalFuture<?>> futs = startCdc(tableName);

        try {
            IntStream.range(0, KEYS_CNT).forEach(insert);

            assertTrue(waitForCondition(waitForTableSize(tableName, KEYS_CNT), getTestTimeout()));

            assertTrue(checkInsert.get());

            executeSql(srcCluster[0], deleteQry);

            assertTrue(waitForCondition(waitForTableSize(tableName, 0), getTestTimeout()));

            IntStream.range(0, KEYS_CNT).forEach(insert);

            IntStream.range(0, KEYS_CNT).forEach(insertForUpdate);

            assertTrue(checkInsertForUpdate.get());
        }
        finally {
            for (IgniteInternalFuture<?> fut : futs)
                fut.cancel();
        }
    }

    /** */
    private List<IgniteInternalFuture<?>> startCdc(String cache) {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (IgniteEx ex : srcCluster) {
            int idx = getTestIgniteInstanceIndex(ex.name());

            futs.add(igniteToPostgres(ex.configuration(), cache, "ignite-src-to-postgres-" + idx));
        }

        return futs;
    }

    /**
     * @param igniteCfg Ignite configuration.
     * @param cache Cache name to stream to kafka.
     * @return Future for Change Data Capture application.
     */
    private IgniteInternalFuture<?> igniteToPostgres(
        IgniteConfiguration igniteCfg,
        String cache,
        String threadName
    ) {
        IgniteToPostgreSqlCdcConsumer cdcCnsmr = new IgniteToPostgreSqlCdcConsumer()
            .setCachesToReplicate(Collections.singleton(cache))
            .setSqlBatchSize(MAX_BATCH_SIZE)
            .setSqlValueSize(MAX_VALUE_SIZE)
            .setOnlyPrimary(onlyPrimary)
            .setDataSource(postgres.getPostgresDatabase());

        CdcConfiguration cdcCfg = new CdcConfiguration();

        cdcCfg.setConsumer(cdcCnsmr);
        cdcCfg.setMetricExporterSpi(new JmxMetricExporterSpi());

        return runAsync(new CdcMain(igniteCfg, null, cdcCfg), threadName);
    }

    /** */
    private GridAbsPredicate waitForTableSize(String tableName, long expSz) {
        return () -> {
            try (ResultSet res = executeOnPostgreSql("SELECT COUNT(*) FROM " + tableName);) {
                res.next();

                long cnt = res.getLong(1);

                return cnt == expSz;
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        };
    }

    /** */
    private List<List<?>> executeSql(IgniteEx node, String sqlText, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(sqlText).setArgs(args), true).getAll();
    }

    /** */
    private ResultSet executeOnPostgreSql(String sqlText) {
        try (Connection conn = postgres.getPostgresDatabase().getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(sqlText);

            return stmt.executeQuery();
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
    }

    /** @return {@code True} if cluster has local updates. */
    private boolean hasLocalUpdates() throws IgniteCheckedException {
        for (IgniteEx srv : srcCluster) {
            WALIterator iter = srv.context().cache().context().wal().replay(null,
                (type, ptr) -> type == WALRecord.RecordType.DATA_RECORD_V2);

            for (IgniteBiTuple<WALPointer, WALRecord> t : iter) {
                Collection<DataEntry> locUpdates = F.view(((DataRecord)t.get2()).writeEntries(),
                    e -> !(e.writeVersion() instanceof GridCacheVersionEx));

                if (!locUpdates.isEmpty())
                    return true;
            }
        }

        return false;
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

        /** */
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
