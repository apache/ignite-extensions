package org.apache.ignite.cdc.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcConsumer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
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
        stopAllGrids();

        cleanPersistenceDir();

        postgres.close();
    }

    /** */
    @Test
    public void testSingleColumnKeyDataReplication() throws Exception {
        executeOnIgnite(srcCluster[0], getCreateTableSqlStatement("T1"));

        String insertQry = "INSERT INTO T1 VALUES(?, ?)";
        String updateQry = "MERGE INTO T1 (ID, NAME) VALUES (?, ?)";

        IntConsumer insert = id -> executeOnIgnite(srcCluster[0], insertQry, id, "Name" + id);
        Supplier<Boolean> checkInsert = () -> checkTable("T1", cnt -> "Name" + cnt);

        IntConsumer update = id -> executeOnIgnite(srcCluster[0], updateQry, id, id + "Name");
        Supplier<Boolean> checkUpdate = () -> checkTable("T1", cnt -> cnt + "Name");

        testDataReplication("T1", insert, checkInsert, update, checkUpdate);
    }

    /** Replication with complex SQL key. Data inserted via SQL. */
    @Test
    public void testMultiColumnKeyDataReplication() throws Exception {
        executeOnIgnite(srcCluster[0], getCreateTableSqlStatementWithCompositeKey("T2"));

        IntConsumer insert = id -> executeOnIgnite(
            srcCluster[0],
            "INSERT INTO T2 (ID, SUBID, NAME, ORGID) VALUES(?, ?, ?, ?)",
            id,
            "SUBID",
            "Name" + id,
            id * 42
        );

        Supplier<Boolean> checkInsert = () -> checkTable("T2", cnt -> "Name" + cnt);

        IntConsumer update = id -> executeOnIgnite(
            srcCluster[0],
            "MERGE INTO T2 (ID, SUBID, NAME, ORGID) VALUES(?, ?, ?, ?)",
            id,
            "SUBID",
            id + "Name",
            id * 42
        );

        Supplier<Boolean> checkUpdate = () -> checkTable("T2", cnt -> cnt + "Name");

        testDataReplication("T2", insert, checkInsert, update, checkUpdate);
    }

    /** Replication with complex SQL key. Data inserted via key-value API. */
    @Test
    public void testMultiColumnKeyDataReplicationWithKeyValue() throws Exception {
        executeOnIgnite(srcCluster[0], getCreateTableSqlStatementWithCompositeKey("T3"));

        IntConsumer insert = id -> srcCluster[0].cache("T3")
            .put(
                new TestKey(id, "SUBID"),
                new TestVal("Name" + id, id * 42)
            );

        Supplier<Boolean> checkInsert = () -> checkTable("T3", cnt -> "Name" + cnt);

        IntConsumer update = id -> srcCluster[0].cache("T3")
            .put(
                new TestKey(id, "SUBID"),
                new TestVal(id + "Name", id * 42)
            );

        Supplier<Boolean> checkUpdate = () -> checkTable("T3", cnt -> cnt + "Name");

        testDataReplication("T3", insert, checkInsert, update, checkUpdate);
    }

    /** */
    private String getCreateTableSqlStatement(String tableName) {
        String backupsStr = mode == PARTITIONED ? "BACKUPS=" + backups + "," : "";

        return "CREATE TABLE " + tableName + " (ID BIGINT PRIMARY KEY, NAME VARCHAR) WITH \"" +
            "CACHE_NAME=" + tableName + "," +
            "VALUE_TYPE=T1Type," +
            "ATOMICITY=" + atomicity.name() + "," +
            backupsStr +
            "TEMPLATE=" + mode.name() + "\";";
    }

    /** */
    private String getCreateTableSqlStatementWithCompositeKey(String tableName) {
        String backupsStr = mode == PARTITIONED ? "BACKUPS=" + backups + "," : "";

        return "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
            "    ID INT NOT NULL, " +
            "    SUBID VARCHAR NOT NULL, " +
            "    NAME VARCHAR, " +
            "    ORGID INT, " +
            "    PRIMARY KEY (ID, SUBID))" +
            "    WITH \"CACHE_NAME=" + tableName + "," +
            "KEY_TYPE=" + TestKey.class.getName() + "," +
            "VALUE_TYPE=" + TestVal.class.getName() + "," +
            "ATOMICITY=" + atomicity.name() + "," +
            backupsStr +
            "TEMPLATE=" + mode.name() + "\";";
    }

    /** */
    private boolean checkTable(String tableName, Function<Long, String> cntToName) {
        try (ResultSet res = executeOnPostgreSql("SELECT NAME FROM " + tableName + " ORDER BY ID")) {
            long cnt = 0;

            String name;

            while (res.next()) {
                name = res.getString("NAME");

                if (!cntToName.apply(cnt).equals(name))
                    return false;

                cnt++;
            }

            return cnt == KEYS_CNT;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private void testDataReplication(
        String tableName,
        IntConsumer insert,
        Supplier<Boolean> checkInsert,
        IntConsumer update,
        Supplier<Boolean> checkUpdate
    ) throws Exception {
        String deleteQry = "DELETE FROM " + tableName;

        List<IgniteInternalFuture<?>> futs = startCdc(Set.of(tableName));

        try {
            IntStream.range(0, KEYS_CNT).forEach(insert);

            assertTrue(waitForCondition(waitForTableSize(tableName, KEYS_CNT), getTestTimeout()));

            assertTrue(checkInsert.get());

            executeOnIgnite(srcCluster[0], deleteQry);

            assertTrue(waitForCondition(waitForTableSize(tableName, 0), getTestTimeout()));

            IntStream.range(0, KEYS_CNT).forEach(insert);

            assertTrue(waitForCondition(waitForTableSize(tableName, KEYS_CNT), getTestTimeout()));

            IntStream.range(0, KEYS_CNT).forEach(update);

            assertTrue(waitForCondition(checkUpdate::get, getTestTimeout()));
        }
        finally {
            for (IgniteInternalFuture<?> fut : futs)
                fut.cancel();
        }
    }

    /** */
    @Test
    public void testMultipleTableDataReplication() throws Exception {
        executeOnIgnite(srcCluster[0], getCreateTableSqlStatement("T4"));
        executeOnIgnite(srcCluster[0], getCreateTableSqlStatement("T5"));
        executeOnIgnite(srcCluster[0], getCreateTableSqlStatement("T6"));

        List<IgniteInternalFuture<?>> futs = startCdc(Set.of("T4", "T5", "T6"));

        try {
            String insertQry = "INSERT INTO %s VALUES(?, ?)";
            String updateQry = "MERGE INTO %s (ID, NAME) VALUES (?, ?)";

            executeOnIgnite(srcCluster[0], String.format(insertQry, "T4"), 1, "Name" + 1);
            executeOnIgnite(srcCluster[0], String.format(updateQry, "T4"), 1, "Name" + 2);
            executeOnIgnite(srcCluster[0], String.format(insertQry, "T4"), 3, "Name" + 1);
            executeOnIgnite(srcCluster[0], String.format(insertQry, "T5"), 4, "Name" + 1);
            executeOnIgnite(srcCluster[0], String.format(insertQry, "T6"), 5, "Name" + 5);
            executeOnIgnite(srcCluster[0], String.format(insertQry, "T6"), 6, "Name" + 6);
            executeOnIgnite(srcCluster[0], String.format(updateQry, "T6"), 5, 5 + "Name");

            assertTrue(waitForCondition(waitForTableSize("T4", 2), getTestTimeout()));
            assertTrue(waitForCondition(waitForTableSize("T5", 1), getTestTimeout()));
            assertTrue(waitForCondition(waitForTableSize("T6", 2), getTestTimeout()));

            assertTrue(checkRow("T4", 1, "Name" + 2));
            assertTrue(checkRow("T4", 3, "Name" + 1));
            assertTrue(checkRow("T5", 4, "Name" + 1));
            assertTrue(checkRow("T6", 5, 5 + "Name"));
            assertTrue(checkRow("T6", 6, "Name" + 6));
        }
        finally {
            for (IgniteInternalFuture<?> fut : futs)
                fut.cancel();
        }
    }

    /** */
    private boolean checkRow(String tableName, int id, String exp) {
        try (ResultSet res = executeOnPostgreSql("SELECT NAME FROM " + tableName + " WHERE ID=" + id)) {
            res.next();

            String name = res.getString("NAME");

            if (!exp.equals(name))
                return false;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }

        return true;
    }

    /** */
    private List<IgniteInternalFuture<?>> startCdc(Set<String> caches) throws IgniteInterruptedCheckedException {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        futs.add(igniteToPostgres(
            srcCluster[0].configuration(),
            caches,
            "ignite-src-to-postgres-0",
            true)
        );

        assertTrue(waitForCondition(waitForTablesCreatedOnPostgres(caches), getTestTimeout()));

        futs.add(igniteToPostgres(
            srcCluster[1].configuration(),
            caches,
            "ignite-src-to-postgres-1",
            false)
        );

        return futs;
    }

    /**
     * @param igniteCfg Ignite configuration.
     * @param caches Cache name set to stream to PostgreSql.
     * @param createTables Create tables flag.
     * @return Future for Change Data Capture application.
     */
    private IgniteInternalFuture<?> igniteToPostgres(
        IgniteConfiguration igniteCfg,
        Set<String> caches,
        String threadName,
        boolean createTables
    ) {
        IgniteToPostgreSqlCdcConsumer cdcCnsmr = new IgniteToPostgreSqlCdcConsumer()
            .setCaches(caches)
            .setMaxBatchSize(MAX_BATCH_SIZE)
            .setOnlyPrimary(onlyPrimary)
            .setDataSource(postgres.getPostgresDatabase())
            .setCreateTables(createTables);

        CdcConfiguration cdcCfg = new CdcConfiguration();

        cdcCfg.setConsumer(cdcCnsmr);
        cdcCfg.setMetricExporterSpi(new JmxMetricExporterSpi());

        return runAsync(new CdcMain(igniteCfg, null, cdcCfg), threadName);
    }

    /** */
    private GridAbsPredicate waitForTablesCreatedOnPostgres(Set<String> caches) {
        return () -> {
            String sql = "SELECT EXISTS (" +
                "  SELECT 1 FROM information_schema.tables " +
                "  WHERE table_name = '%s'" +
                ")";

            for (String cache : caches) {
                try (ResultSet rs = executeOnPostgreSql(String.format(sql, cache.toLowerCase()))) {
                    rs.next();

                    if (!rs.getBoolean(1))
                        return false;
                }
                catch (SQLException e) {
                    log.error(e.getMessage(), e);

                    throw new IgniteException(e);
                }
            }

            return true;
        };
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
    private void executeOnIgnite(IgniteEx node, String sqlText, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sqlText).setArgs(args);

        try (FieldsQueryCursor<List<?>> cursor = node.context().query().querySqlFields(qry, true)) {
            cursor.getAll();
        }
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
