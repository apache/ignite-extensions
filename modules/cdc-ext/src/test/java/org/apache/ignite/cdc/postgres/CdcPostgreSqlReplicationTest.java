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

package org.apache.ignite.cdc.postgres;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcConsumer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class CdcPostgreSqlReplicationTest extends CdcPostgreSqlReplicationAbstractTest {
    /** */
    private static final int BACKUP = 0;

    /** */
    private static final String CACHE_MODE = "PARTITIONED";

    /** */
    @Parameterized.Parameter()
    public CacheAtomicityMode atomicity;

    /** */
    @Parameterized.Parameter(1)
    public boolean createTables;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "atomicity={0}, createTables={1}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode atomicity : EnumSet.of(ATOMIC, TRANSACTIONAL)) {
            for (boolean createTables : new boolean[] {true, false})
                params.add(new Object[] {atomicity, createTables});
        }

        return params;
    }

    /** */
    protected IgniteEx src;

    /** */
    protected EmbeddedPostgres postgres;

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
    @Override protected IgniteToPostgreSqlCdcConsumer getCdcConsumerConfiguration() {
        IgniteToPostgreSqlCdcConsumer cdcCfg = super.getCdcConsumerConfiguration();

        cdcCfg.setCreateTables(createTables);

        return cdcCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        src = startGrid(0);

        src.cluster().state(ClusterState.ACTIVE);

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
    public void testSingleColumnKeyDataReplicationWithPrimaryFirst() throws Exception {
        testSingleColumnKeyDataReplication(false);
    }

    /** */
    @Test
    public void testSingleColumnKeyDataReplicationWithPrimaryLast() throws Exception {
        testSingleColumnKeyDataReplication(true);
    }

    /** */
    public void testSingleColumnKeyDataReplication(boolean isPrimaryLast) throws Exception {
        String[] tableFields;

        String insertQry = "INSERT INTO T1 VALUES(?, ?)";
        String updateQry;

        IntConsumer insert;
        IntConsumer update;

        if (isPrimaryLast) {
            tableFields = new String[] {"NAME VARCHAR(20)", "ID BIGINT PRIMARY KEY"};

            updateQry = "MERGE INTO T1 (NAME, ID) VALUES (?, ?)";

            insert = id -> executeOnIgnite(src, insertQry, "Name" + id, id);
            update = id -> executeOnIgnite(src, updateQry, id + "Name", id);
        }
        else {
            tableFields = new String[] {"ID BIGINT PRIMARY KEY", "NAME VARCHAR(20)"};

            updateQry = "MERGE INTO T1 (ID, NAME) VALUES (?, ?)";

            insert = id -> executeOnIgnite(src, insertQry, id, "Name" + id);
            update = id -> executeOnIgnite(src, updateQry, id, id + "Name");
        }

        createTable("T1", tableFields, null, null, null);

        Supplier<Boolean> checkInsert = () -> checkSingleColumnKeyTable(id -> "Name" + id);

        Supplier<Boolean> checkUpdate = () -> checkSingleColumnKeyTable(id -> id + "Name");

        testDataReplication("T1", insert, checkInsert, update, checkUpdate);
    }

    /** */
    private boolean checkSingleColumnKeyTable(Function<Long, String> idToName) {
        String qry = "SELECT ID, NAME FROM T1 ORDER BY ID";

        try (ResultSet res = selectOnPostgreSql(postgres, qry)) {
            long cnt = 0;

            long id;
            String curName;

            while (res.next()) {
                id = res.getLong("ID");
                curName = res.getString("NAME");

                if (!idToName.apply(id).equals(curName) || cnt != id)
                    return false;

                cnt++;
            }

            return cnt == KEYS_CNT;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** Replication with complex SQL key. Data inserted via SQL. */
    @Test
    public void testMultiColumnKeyDataReplicationWithSql() throws Exception {
        IntConsumer insert = id -> executeOnIgnite(
            src,
            "INSERT INTO T2 (ID, SUBID, NAME, VAL) VALUES(?, ?, ?, ?)",
            id,
            "SUBID",
            "Name" + id,
            id * 42
        );

        IntConsumer update = id -> executeOnIgnite(
            src,
            "MERGE INTO T2 (ID, SUBID, NAME, VAL) VALUES(?, ?, ?, ?)",
            id,
            "SUBID",
            id + "Name",
            id + 42
        );

        testMultiColumnKeyDataReplication("T2", insert, update);
    }

    /** Replication with complex SQL key. Data inserted via key-value API. */
    @Test
    public void testMultiColumnKeyDataReplicationWithKeyValue() throws Exception {
        IntConsumer insert = id -> src.cache("T3")
            .put(
                new TestKey(id, "SUBID"),
                new TestVal("Name" + id, id * 42)
            );

        IntConsumer update = id -> src.cache("T3")
            .put(
                new TestKey(id, "SUBID"),
                new TestVal(id + "Name", id + 42)
            );

        testMultiColumnKeyDataReplication("T3", insert, update);
    }

    /** */
    public void testMultiColumnKeyDataReplication(String tableName, IntConsumer insert, IntConsumer update) throws Exception {
        String[] tableFields = new String[] {
            "ID INT NOT NULL",
            "SUBID VARCHAR(15) NOT NULL",
            "NAME VARCHAR",
            "VAL INT"
        };

        String constraint = "PRIMARY KEY (ID, SUBID)";

        createTable(tableName, tableFields, constraint, TestKey.class.getName(), TestVal.class.getName());

        Supplier<Boolean> checkInsert = () -> checkMultiColumnKeyTable(tableName, id -> "Name" + id, id -> id * 42);

        Supplier<Boolean> checkUpdate = () -> checkMultiColumnKeyTable(tableName, id -> id + "Name", id -> id + 42);

        testDataReplication(tableName, insert, checkInsert, update, checkUpdate);
    }

    /** */
    private boolean checkMultiColumnKeyTable(
        String tableName,
        Function<Integer, String> idToName,
        Function<Integer, Integer> idToVal
    ) {
        String qry = "SELECT ID, NAME, VAL FROM " + tableName + " ORDER BY ID";

        try (ResultSet res = selectOnPostgreSql(postgres, qry)) {
            long cnt = 0;

            int id;
            String curName;
            int curVal;

            while (res.next()) {
                id = res.getInt("ID");
                curName = res.getString("NAME");
                curVal = res.getInt("VAL");

                if (!idToVal.apply(id).equals(curVal) || !idToName.apply(id).equals(curName) || cnt != id)
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
        IgniteInternalFuture<?> fut = startCdc(Stream.of(tableName).collect(Collectors.toSet()));

        try {
            IntStream.range(0, KEYS_CNT).forEach(insert);

            assertTrue(waitForCondition(waitForTableSize(postgres, tableName, KEYS_CNT), getTestTimeout()));

            assertTrue(checkInsert.get());

            executeOnIgnite(src, "DELETE FROM " + tableName);

            assertTrue(waitForCondition(waitForTableSize(postgres, tableName, 0), getTestTimeout()));

            IntStream.range(0, KEYS_CNT).forEach(insert);

            assertTrue(waitForCondition(waitForTableSize(postgres, tableName, KEYS_CNT), getTestTimeout()));

            rangeWithDuplicates(0, KEYS_CNT).forEach(update);

            assertTrue(waitForCondition(checkUpdate::get, getTestTimeout()));
        }
        finally {
            fut.cancel();
        }
    }

    /**
     * @param startInclusive Start inclusive.
     * @param endExclusive End exclusive.
     */
    private IntStream rangeWithDuplicates(int startInclusive, int endExclusive) {
        List<Integer> duplicatedKeys = IntStream.concat(
                IntStream.range(startInclusive, endExclusive),
                IntStream.range(startInclusive, endExclusive)
            )
            .boxed()
            .collect(Collectors.toList());

        Collections.shuffle(duplicatedKeys);

        return duplicatedKeys.stream().mapToInt(Integer::intValue);
    }

    /** */
    @Test
    public void testMultipleTableDataReplication() throws Exception {
        String[] tableFields = new String[] {"ID BIGINT PRIMARY KEY", "NAME VARCHAR"};

        createTable("T4", tableFields, null, null, null);
        createTable("T5", tableFields, null, null, null);
        createTable("T6", tableFields, null, null, null);

        IgniteInternalFuture<?> fut = startCdc(Stream.of("T4", "T5", "T6").collect(Collectors.toSet()));

        try {
            String insertQry = "INSERT INTO %s VALUES(?, ?)";
            String updateQry = "MERGE INTO %s (ID, NAME) VALUES (?, ?)";

            executeOnIgnite(src, String.format(insertQry, "T4"), 1, "Name" + 1);

            assertTrue(waitForCondition(waitForTableSize(postgres, "T4", 1), getTestTimeout()));

            executeOnIgnite(src, String.format(updateQry, "T4"), 1, "Name" + 2);
            executeOnIgnite(src, String.format(insertQry, "T4"), 3, "Name" + 1);
            executeOnIgnite(src, String.format(insertQry, "T5"), 4, "Name" + 1);
            executeOnIgnite(src, String.format(insertQry, "T6"), 5, "Name" + 5);
            executeOnIgnite(src, String.format(insertQry, "T6"), 6, "Name" + 6);
            executeOnIgnite(src, String.format(updateQry, "T6"), 5, 5 + "Name");

            assertTrue(waitForCondition(waitForTableSize(postgres, "T4", 2), getTestTimeout()));
            assertTrue(waitForCondition(waitForTableSize(postgres, "T5", 1), getTestTimeout()));
            assertTrue(waitForCondition(waitForTableSize(postgres, "T6", 2), getTestTimeout()));

            assertTrue(checkRow(postgres, "T4", "NAME", "Name" + 2, "ID=1"));
            assertTrue(checkRow(postgres, "T4", "NAME", "Name" + 1, "ID=3"));
            assertTrue(checkRow(postgres, "T5", "NAME", "Name" + 1, "ID=4"));
            assertTrue(checkRow(postgres, "T6", "NAME", 5 + "Name", "ID=5"));
            assertTrue(checkRow(postgres, "T6", "NAME", "Name" + 6, "ID=6"));
        }
        finally {
            fut.cancel();
        }
    }

    /** */
    private IgniteInternalFuture<?> startCdc(Set<String> caches) throws IgniteInterruptedCheckedException {
        IgniteInternalFuture<?> fut = startIgniteToPostgreSqlCdcConsumer(src.configuration(), caches, postgres.getPostgresDatabase());

        assertTrue(waitForCondition(waitForTablesCreatedOnPostgres(postgres, caches), getTestTimeout()));

        return fut;
    }

    /** */
    private void createTable(String tableName, String[] fields, String constraint, String keyClsName, String valClsName) {
        StringBuilder fieldsBldr = new StringBuilder();

        A.notEmpty(fields, "Empty fields declaration.");

        for (int i = 0; i < fields.length; ++i) {
            fieldsBldr.append(fields[i]);

            if (i < fields.length - 1)
                fieldsBldr.append(",");
        }

        String constraintQry = constraint == null ? "" : ", " + constraint;

        String createQry = "CREATE TABLE IF NOT EXISTS " + tableName +
            " (" + fieldsBldr + constraintQry + ")";

        String createQryWithArgs = createQry +
            "    WITH \"CACHE_NAME=" + tableName + "," +
            (keyClsName == null ? "" : "KEY_TYPE=" + keyClsName + ",") +
            (valClsName == null ? "" : "VALUE_TYPE=" + valClsName + ",") +
            "ATOMICITY=" + atomicity.name() + "," +
            "BACKUPS=" + BACKUP + "," +
            "TEMPLATE=" + CACHE_MODE + "\";";

        executeOnIgnite(src, createQryWithArgs);

        if (!createTables)
            executeOnPostgreSql(postgres, "CREATE TABLE IF NOT EXISTS " + tableName +
                " (" + fieldsBldr + ", version BYTEA NOT NULL" + constraintQry + ")");
    }

    /** */
    @Test
    public void testQueryEntityError() throws IgniteCheckedException {
        QueryEntity qryOnlyValType = new QueryEntity()
            .setTableName("qryOnlyValType")
            .setValueType("org.apache.ignite.cdc.postgres.TestVal");

        testQueryEntityReplicationError(qryOnlyValType);

        QueryEntity qryOnlyValName = new QueryEntity()
            .setTableName("qryOnlyValName")
            .setValueFieldName("name")
            .addQueryField("name", String.class.getName(), null)
            .addQueryField("val", Integer.class.getName(), null);

        testQueryEntityReplicationError(qryOnlyValName);
    }

    /** */
    private void testQueryEntityReplicationError(QueryEntity qryEntity) throws IgniteInterruptedCheckedException {
        CacheConfiguration<Integer, TestVal> ccfg = new CacheConfiguration<Integer, TestVal>(qryEntity.getTableName())
            .setQueryEntities(Collections.singletonList(qryEntity));

        src.getOrCreateCache(ccfg);

        IgniteInternalFuture<?> fut = startIgniteToPostgreSqlCdcConsumer(
            src.configuration(),
            new HashSet<>(Collections.singletonList(qryEntity.getTableName())),
            postgres.getPostgresDatabase()
        );

        waitForCondition(fut::isDone, getTestTimeout());

        checkFutureEndWithError(fut);
    }

    /** */
    protected void checkFutureEndWithError(IgniteInternalFuture<?> fut) {
        assertTrue(fut.error() != null);
    }

    /** */
    @Test
    public void testQueryEntityWithKeyValueFieldNames() throws IgniteCheckedException {
        QueryEntity qryEntity = new QueryEntity()
            .setTableName("qryKeyValName")
            .setKeyFieldName("id")
            .setValueFieldName("name")
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("name", String.class.getName(), null)
            .addQueryField("val", Integer.class.getName(), null);

        int id = 2;

        String name = "1";
        int val = 3;

        Function<ResultSet, Boolean> check = res -> {
            try {
                assertTrue(res.next());

                int actId = res.getInt("id");
                String actName = res.getString("name");
                int actVal = res.getInt("val");

                return actId == id && Objects.equals(actName, name) && actVal == val;
            }
            catch (Exception e) {
                return false;
            }
        };

        String[] tableFields = new String[] {
            "ID INT NOT NULL",
            "NAME VARCHAR",
            "VAL INT"
        };

        String constraint = "PRIMARY KEY (ID)";

        testQueryEntityReplicationSuccess(qryEntity, tableFields, constraint, id, new TestVal(name, val), check);
    }

    /** */
    @Test
    public void testQueryEntityWithKeyFieldsAndValName() throws IgniteCheckedException {
        QueryEntity qryEntity = new QueryEntity()
            .setTableName("qryKeyFieldsValName")
            .setKeyFields(new HashSet<>(Arrays.asList("id", "subId")))
            .setValueFieldName("name")
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("subId", String.class.getName(), null)
            .addQueryField("name", String.class.getName(), null)
            .addQueryField("val", Integer.class.getName(), null);

        int id = 4;
        String subId = "foobar";

        String name = "5";
        int val = 0;

        Function<ResultSet, Boolean> check = res -> {
            try {
                assertTrue(res.next());

                int actId = res.getInt("id");
                String actSubId = res.getString("subId");
                String actName = res.getString("name");
                int actVal = res.getInt("val");

                return actId == id && Objects.equals(actSubId, subId) && Objects.equals(actName, name) && actVal == val;
            }
            catch (Exception e) {
                return false;
            }
        };

        String[] tableFields = new String[] {
            "ID INT NOT NULL",
            "SUBID VARCHAR(15) NOT NULL",
            "NAME VARCHAR",
            "VAL INT"
        };

        String constraint = "PRIMARY KEY (ID, SUBID)";

        testQueryEntityReplicationSuccess(
            qryEntity,
            tableFields,
            constraint,
            new TestKey(id, subId),
            new TestVal(name, val),
            check
        );
    }

    /** */
    @Test
    public void testQueryEntityWithKeyNameValueTypeAndFields() throws IgniteCheckedException {
        QueryEntity qryEntity = new QueryEntity()
            .setTableName("TESTING")
            .setKeyFieldName("id")
            .setValueType("org.apache.ignite.cdc.postgres.TestVal")
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("name", String.class.getName(), null)
            .addQueryField("val", Integer.class.getName(), null);

        int id = 3;

        String name = "test";
        int val = 9;

        Function<ResultSet, Boolean> check = res -> {
            try {
                assertTrue(res.next());

                int actId = res.getInt("id");
                String actName = res.getString("name");
                int actVal = res.getInt("val");

                return actId == id && Objects.equals(actName, name) && actVal == val;
            }
            catch (Exception e) {
                return false;
            }
        };

        String[] tableFields = new String[] {
            "ID INT NOT NULL",
            "NAME VARCHAR",
            "VAL INT"
        };

        String constraint = "PRIMARY KEY (ID)";

        testQueryEntityReplicationSuccess(qryEntity, tableFields, constraint, id, new TestVal(name, val), check);
    }

    /** */
    public <K, V> void testQueryEntityReplicationSuccess(
        QueryEntity qryEntity,
        String[] fields,
        String constraint,
        K key,
        V val,
        Function<ResultSet, Boolean> checkTable
    ) throws IgniteCheckedException {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<K, V>(qryEntity.getTableName())
            .setQueryEntities(Collections.singletonList(qryEntity));

        IgniteCache<K, V> cache = src.getOrCreateCache(ccfg);

        if (!createTables) {
            StringBuilder fieldsBldr = new StringBuilder();

            A.notEmpty(fields, "Empty fields declaration.");

            for (int i = 0; i < fields.length; ++i) {
                fieldsBldr.append(fields[i]);

                if (i < fields.length - 1)
                    fieldsBldr.append(",");
            }

            String constraintQry = constraint == null ? "" : ", " + constraint;

            executeOnPostgreSql(postgres, "CREATE TABLE IF NOT EXISTS " + qryEntity.getTableName() +
                " (" + fieldsBldr + ", version BYTEA NOT NULL" + constraintQry + ")");
        }

        cache.put(key, val);

        IgniteInternalFuture<?> fut = startCdc(new HashSet<>(Collections.singletonList(qryEntity.getTableName())));

        assertTrue(waitForCondition(waitForTableSize(postgres, qryEntity.getTableName(), 1), getTestTimeout()));

        ResultSet set = selectOnPostgreSql(postgres, "SELECT * FROM " + qryEntity.getTableName());

        assertTrue(checkTable.apply(set));

        fut.cancel();
    }

    /** */
    private static class TestKey {
        /** */
        private final int id;

        /** */
        private final String subId;

        /** */
        public TestKey(int id, String subId) {
            this.id = id;
            this.subId = subId;
        }
    }

    /** */
    private static class TestVal {
        /** */
        private final String name;

        /** */
        private final int val;

        /** */
        public TestVal(String name, int val) {
            this.name = name;
            this.val = val;
        }
    }
}
