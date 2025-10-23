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

package org.apache.ignite.cdc.postgresql;

import java.io.File;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class JavaToSqlTypeMapperTest extends CdcPostgreSqlReplicationAbstractTest {
    /** Type mapping. */
    private static final Map<Class<?>, String> TYPE_MAPPING = Map.of(
        Byte.class, "int2",
        Short.class, "int2",
        Integer.class, "int4",
        Long.class, "int8",
        OffsetDateTime.class, "timestamptz"
    );

    /** */
    private static final int PRECISION_EXTRA_TIME = 9;

    /** */
    private static final int PRECISION_EXTRA_DATE_TIME = 23;

    /** */
    private static final String KEY_COLUMN_NAME = "id";

    /** */
    private static final String VAL_COLUMN_NAME = "value";

    /** */
    private final JavaToSqlTypeMapper javaToSqlTypeMapper = new JavaToSqlTypeMapper();

    /** */
    private IgniteEx src;

    /** */
    private EmbeddedPostgres postgres;

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

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteToPostgreSqlCdcConsumer getCdcConsumerConfiguration() {
        IgniteToPostgreSqlCdcConsumer cdcCfg = super.getCdcConsumerConfiguration();

        cdcCfg.setCreateTables(true);

        return cdcCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        src = startGrid(0);

        src.cluster().state(ClusterState.ACTIVE);

        // Do not create directory implicitly in /tmp due to possible cleanup problems.
        File pgDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "embedded-pg", false, false);

        postgres = EmbeddedPostgres.builder()
            .setOverrideWorkingDirectory(pgDir)
            .start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        postgres.close();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void javaToPostgreSqlTypesMappingTest() throws Exception {
        Set<TestCase> valuesToReplicate = getValuesToReplicate();

        Set<String> cachesToReplicate = valuesToReplicate.stream().map(TestCase::tableName).collect(Collectors.toSet());

        IgniteInternalFuture<?> fut = startIgniteToPostgreSqlCdcConsumer(
            src.configuration(),
            cachesToReplicate,
            postgres.getPostgresDatabase()
        );

        valuesToReplicate.forEach(this::createCacheWithValue);

        assertTrue(waitForCondition(waitForTablesCreatedOnPostgres(postgres, cachesToReplicate), getTestTimeout()));

        for (String cache : cachesToReplicate)
            assertTrue(waitForCondition(waitForTableSize(postgres, cache, 1), getTestTimeout()));

        valuesToReplicate.forEach(this::checkTable);

        fut.cancel();
    }

    /** */
    private Set<TestCase> getValuesToReplicate() {
        LocalDateTime locDateTime = LocalDateTime.of(1999, 8, 6, 23, 30, 3);

        return Set.of(
            new TestCase("string"),
            new TestCase("string", 10),
            new TestCase(5),
            new TestCase(6L),
            new TestCase(true),
            new TestCase(23.0),
            new TestCase(23.0, 4),
            new TestCase(23.343, 10, 3),
            new TestCase(23.0f),
            new TestCase(33.0f, 4),
            new TestCase(23.12f, 5, 2),
            new TestCase(new BigDecimal("1")),
            new TestCase(new BigDecimal("323"), 3),
            new TestCase(new BigDecimal("1.623"), 5, 3),
            new TestCase((short)23),
            new TestCase((byte)33),
            new TestCase(new Time(43830000)),
            new TestCase(new Time(43830123), 3),
            new TestCase(new Timestamp(1755000630000L)),
            new TestCase(new Timestamp(1755000630123L), 3),
            new TestCase(new Date(933960600000L)),
            new TestCase(UUID.randomUUID()),
            new TestCase(LocalDate.of(1999, 8, 6)),
            new TestCase(LocalTime.of(12, 12, 12)),
            new TestCase(LocalTime.of(12, 12, 12), 6),
            new TestCase(locDateTime),
            new TestCase(locDateTime, 4),
            new TestCase(OffsetTime.now()),
            new TestCase(OffsetTime.now(), 30),
            new TestCase(OffsetDateTime.of(locDateTime, ZoneOffset.ofHours(3))),
            new TestCase(new byte[]{1, 2, 3, 4})
        );
    }

    /** */
    private void createCacheWithValue(TestCase testCase) {
        String clsName = testCase.value().getClass().getName();

        QueryEntity qryEntity = new QueryEntity()
            .setTableName(testCase.tableName())
            .setKeyFieldName(KEY_COLUMN_NAME)
            .setValueFieldName(VAL_COLUMN_NAME)
            .addQueryField(KEY_COLUMN_NAME, Integer.class.getName(), null)
            .addQueryField(VAL_COLUMN_NAME, clsName, null);

        if (testCase.precision() != null)
            qryEntity.setFieldsPrecision(Map.of(VAL_COLUMN_NAME, testCase.precision()));

        if (testCase.scale() != null)
            qryEntity.setFieldsScale(Map.of(VAL_COLUMN_NAME, testCase.scale()));

        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<Integer, Object>(qryEntity.getTableName())
            .setQueryEntities(Collections.singletonList(qryEntity));

        try (IgniteCache<Integer, Object> cache = src.getOrCreateCache(ccfg)) {
            cache.put(1, testCase.value());
        }
    }

    /** */
    private void checkTable(TestCase testCase) {
        selectOnPostgreSqlAndAct(postgres, "SELECT * FROM " + testCase.tableName(), (res) -> {
            assertTrue(res.next());

            checkValue(res, testCase.value());

            ResultSetMetaData meta = res.getMetaData();

            String actTypeName = meta.getColumnTypeName(2);

            checkType(testCase.value(), actTypeName);

            if (testCase.precision() != null)
                checkValueMeta(meta, testCase.value(), testCase.precision(), testCase.scale());

            return true;
        });
    }

    /** */
    private void checkValue(ResultSet res, Object val) throws SQLException {
        if (val instanceof Boolean)
            assert Objects.equals(res.getBoolean(VAL_COLUMN_NAME), val);
        else if (val instanceof Double)
            assert Objects.equals(res.getDouble(VAL_COLUMN_NAME), val);
        else if (val instanceof Float)
            assert Objects.equals(res.getFloat(VAL_COLUMN_NAME), val);
        else if (val instanceof BigDecimal)
            assert Objects.equals(res.getBigDecimal(VAL_COLUMN_NAME), val);
        else if (val instanceof Timestamp )
            assert Objects.equals(res.getTimestamp(VAL_COLUMN_NAME), val);
        else if (val instanceof LocalDateTime)
            assert Objects.equals(res.getTimestamp(VAL_COLUMN_NAME), Timestamp.valueOf((LocalDateTime)val));
        else if (val instanceof Date)
            assert Objects.equals(res.getTimestamp(VAL_COLUMN_NAME).getTime(), ((Date)val).getTime());
        else if (val instanceof OffsetDateTime)
            assert Objects.equals(res.getObject(VAL_COLUMN_NAME, OffsetDateTime.class).withOffsetSameInstant(ZoneOffset.UTC),
                ((OffsetDateTime)val).withOffsetSameInstant(ZoneOffset.UTC));
        else if (val instanceof byte[])
            assert Arrays.equals(res.getBytes(VAL_COLUMN_NAME), (byte[])val);
        else
            assert Objects.equals(res.getString(VAL_COLUMN_NAME), val.toString());
    }

    /**
     * Verifies that the PostgreSQL type name matches the expected mapping for the given Java value.
     *
     * @param val          Java value to check.
     * @param actTypeName  Actual PostgreSQL type name from metadata.
     */
    private void checkType(Object val, String actTypeName) {
        String actual = actTypeName.toLowerCase();
        String expected = TYPE_MAPPING.get(val.getClass());

        if (expected == null) {
            expected = javaToSqlTypeMapper
                .renderSqlType(val.getClass().getName(), null, null)
                .toLowerCase();
        }

        assert Objects.equals(actual, expected);
    }

    /**
     * Verifies that the precision and scale reported by {@link ResultSetMetaData}
     * match the expected values for the given value type.
     *
     * @param meta      The result set metadata to check.
     * @param val       The value whose type determines expected precision/scale.
     * @param precision The expected base precision (without temporal adjustments).
     * @param scale     The expected scale, or {@code null} if not applicable.
     * @throws SQLException If metadata retrieval fails.
     */
    private void checkValueMeta(ResultSetMetaData meta, Object val, Integer precision, Integer scale) throws SQLException {
        int actualPrecision = meta.getPrecision(2);
        int actualScale = meta.getScale(2);

        int expectedPrecision = precision;

        if (val instanceof Time || val instanceof LocalTime)
            expectedPrecision += PRECISION_EXTRA_TIME;
        else if (val instanceof LocalDateTime || val instanceof Date)
            expectedPrecision += PRECISION_EXTRA_DATE_TIME;

        assert Objects.equals(actualPrecision, expectedPrecision);

        if (val instanceof LocalTime || val instanceof LocalDateTime || val instanceof Date)
            assert Objects.equals(actualScale, precision);
        else
            assert scale == null || Objects.equals(actualScale, scale);
    }

    /** */
    private static class TestCase {
        /** */
        private final Object val;

        /** */
        private Integer precision = null;

        /** */
        private Integer scale = null;

        /** */
        private TestCase(Object val) {
            this.val = val;
        }

        /** */
        private TestCase(Object val, Integer precision) {
            this.val = val;
            this.precision = precision;
        }

        /** */
        private TestCase(Object val, Integer precision, Integer scale) {
            this.val = val;
            this.precision = precision;
            this.scale = scale;
        }

        /** */
        public Object value() {
            return val;
        }

        /** */
        public Integer precision() {
            return precision;
        }

        /** */
        public Integer scale() {
            return scale;
        }

        /** */
        public String tableName() {
            return val.getClass().getName().replace('.', '_').replace('[', '_')
                + (precision != null ? '_' + precision : "") + (scale != null ? '_' + scale : "");
        }
    }
}
