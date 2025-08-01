package org.apache.ignite.cdc.postgresql;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.junit.Test;

import static org.apache.ignite.cdc.postgresql.JavaToSqlTypeMapper.toBigDecimal;
import static org.apache.ignite.cdc.postgresql.JavaToSqlTypeMapperTest.NumericMeta.NO_NUMERIC_META;
import static org.apache.ignite.cdc.postgresql.JavaToSqlTypeMapperTest.NumericMeta.PRECISION_AND_SCALE;
import static org.apache.ignite.cdc.postgresql.JavaToSqlTypeMapperTest.NumericMeta.PRECISION_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class JavaToSqlTypeMapperTest extends CdcPostgreSqlReplicationAbstractTest {
    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteToPostgreSqlCdcConsumer getCdcConsumerConfiguration() {
        IgniteToPostgreSqlCdcConsumer cdcCfg = super.getCdcConsumerConfiguration();

        cdcCfg.setCreateTables(true);

        return cdcCfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    public void javaToPostgreSqlTypesMappingTest() throws Exception {
        try (IgniteEx src = startGrid(0); EmbeddedPostgres postgres = EmbeddedPostgres.builder().start()) {
            src.cluster().state(ClusterState.ACTIVE);

            Set<String> supportedTypes = Arrays.stream(JavaToSqlTypeMapper.JavaToSqlType.values())
                .map(JavaToSqlTypeMapper.JavaToSqlType::getJavaTypeName)
                .filter(name -> !name.equals(Object.class.getName()))
                .map(clsName -> clsName.replace('.', '_').replace('[', '_'))
                .collect(Collectors.toSet());

            Set<String> tableNames = supportedTypes.stream()
                .flatMap(clsName -> Stream.of(
                    clsName,
                    clsName + "_WithPrecision",
                    clsName + "_WithPrecision_WithScale"
                ))
                .collect(Collectors.toSet());

            IgniteInternalFuture<?> fut = startIgniteToPostgreSqlCdcConsumer(
                src.configuration(),
                tableNames,
                postgres.getPostgresDatabase()
            );

            createCacheAndCheck(src, postgres, "string", null, null, NO_NUMERIC_META);
            createCacheAndCheck(src, postgres, "string", 10, null, PRECISION_ONLY);
            createCacheAndCheck(src, postgres, "string", 10, 10, PRECISION_ONLY);

            createCacheAndCheck(src, postgres, 5, null, null, NO_NUMERIC_META);

            createCacheAndCheck(src, postgres, 6L, null, null, NO_NUMERIC_META);

            createCacheAndCheck(src, postgres, true, null, null, NO_NUMERIC_META);

            createCacheAndCheck(src, postgres, 23.0, null, null, NO_NUMERIC_META);
            createCacheAndCheck(src, postgres, 23.0, 20, null, PRECISION_ONLY);
            createCacheAndCheck(src, postgres, 23.343, 10, 3, PRECISION_AND_SCALE);

            createCacheAndCheck(src, postgres, 23.0f, null, null, NO_NUMERIC_META);
            createCacheAndCheck(src, postgres, 23.0f, 10, null, PRECISION_ONLY);
            createCacheAndCheck(src, postgres, 23.12f, 5, 2, PRECISION_AND_SCALE);

            createCacheAndCheck(src, postgres, new BigDecimal("123.456"), null, null, NO_NUMERIC_META);
            createCacheAndCheck(src, postgres, new BigDecimal("1234"), 4, null, PRECISION_ONLY);
            createCacheAndCheck(src, postgres, new BigDecimal("1.623"), 4, 3, PRECISION_AND_SCALE);

            createCacheAndCheck(src, postgres, (short)23, null, null, NO_NUMERIC_META);

            createCacheAndCheck(src, postgres, (byte)33, null, null, NO_NUMERIC_META);

            createCacheAndCheck(src, postgres, new java.sql.Date(933883200000L), null, null, NO_NUMERIC_META);

            java.sql.Time sqlTime = new java.sql.Time(933960600000L);

            createCacheAndCheck(src, postgres, sqlTime, null, null, NO_NUMERIC_META);
            createCacheAndCheck(src, postgres, sqlTime, 6, null, PRECISION_ONLY);
            createCacheAndCheck(src, postgres, sqlTime, 6, 1, PRECISION_ONLY);

            createCacheAndCheck(src, postgres, new java.sql.Timestamp(933960600000L), null, null, NO_NUMERIC_META);
            createCacheAndCheck(src, postgres, new java.sql.Timestamp(933960600000L), 2, null, PRECISION_ONLY);
            createCacheAndCheck(src, postgres, new java.sql.Timestamp(933960612345L), 4, 1, PRECISION_ONLY);

            createCacheAndCheck(src, postgres, new java.util.Date(933960600000L), null, null, NO_NUMERIC_META);
            createCacheAndCheck(src, postgres, new java.util.Date(933960600000L), 2, null, PRECISION_ONLY);
            createCacheAndCheck(src, postgres, new java.util.Date(933960612345L), 4, 1, PRECISION_ONLY);

            createCacheAndCheck(src, postgres, UUID.randomUUID(), null, null, NO_NUMERIC_META);

            createCacheAndCheck(src, postgres, Period.of(1, 2, 3), null, null, NO_NUMERIC_META);

            createCacheAndCheck(src, postgres, Duration.ofNanos(1123456789), null, null, NO_NUMERIC_META);

            LocalDate locDate = LocalDate.of(1999, 8, 6);

            createCacheAndCheck(src, postgres, locDate, null, null, NO_NUMERIC_META);

            LocalTime locTime = LocalTime.of(12, 12, 12);

            createCacheAndCheck(src, postgres, locTime, null, null, NO_NUMERIC_META);
            createCacheAndCheck(src, postgres, locTime, 6, null, PRECISION_ONLY);
            createCacheAndCheck(src, postgres, locTime, 6, 1, PRECISION_ONLY);

            LocalDateTime locDateTime = LocalDateTime.of(
                1999, 8, 6,
                23, 30, 3,
                965_615_000
            );

            createCacheAndCheck(src, postgres, locDateTime, null, null, NO_NUMERIC_META);
            createCacheAndCheck(src, postgres, locDateTime, 2, null, PRECISION_ONLY);
            createCacheAndCheck(src, postgres, locDateTime, 4, 1, PRECISION_ONLY);

            createCacheAndCheck(src, postgres, OffsetTime.now(), null, null, NO_NUMERIC_META);
            createCacheAndCheck(src, postgres, OffsetTime.now(), 30, null, PRECISION_ONLY);
            createCacheAndCheck(src, postgres, OffsetTime.now(), 30, 1, PRECISION_ONLY);

            OffsetDateTime dateWithOffset = OffsetDateTime.of(locDateTime, ZoneOffset.ofHours(3));

            createCacheAndCheck(src, postgres, dateWithOffset, null, null, NO_NUMERIC_META);

            createCacheAndCheck(src, postgres, new byte[]{1, 2, 3, 4}, null, null, NO_NUMERIC_META);

            for (String tableName : supportedTypes)
                assertTrue(waitForCondition(waitForTableSize(postgres, tableName, 1), getTestTimeout()));

            fut.cancel();
        }
    }

    /**
     *
     */
    private <V> void createCacheAndCheck(
        IgniteEx src,
        EmbeddedPostgres postgres,
        V val,
        Integer precision,
        Integer scale,
        NumericMeta actualMeta
    ) throws IgniteInterruptedCheckedException, SQLException {
        String tableName = val.getClass().getName().replace('.', '_').replace('[', '_') +
            (precision != null ? "_WithPrecision" : "") +
            (scale != null ? "_WithScale" : "");

        IgniteCache<Integer, V> cache = createCache(src, tableName, val.getClass().getName(), precision, scale);

        cache.put(1, val);

        waitForCondition(waitForTablesCreatedOnPostgres(postgres, Set.of(tableName)), getTestTimeout());

        waitForCondition(waitForTableSize(postgres, tableName, 1), getTestTimeout());

        assertTrue(
            selectOnPostgreSqlAndAct(postgres, "SELECT * FROM " + tableName, (res) -> {
                assertTrue(res.next());

                checkValue(res, val);

                ResultSetMetaData meta = res.getMetaData();

                String actTypeName = meta.getColumnTypeName(2);

                checkType(val, actTypeName);

                if (actualMeta != NO_NUMERIC_META)
                    checkValueMeta(meta, val, precision, actualMeta == PRECISION_AND_SCALE ? scale : 0);

                return true;
            })
        );
    }

    /** */
    private void checkValue(ResultSet res, Object val) throws SQLException {
        String actVal = res.getString("value");

        if (val instanceof Boolean)
            assert Objects.equals(res.getBoolean("value"), val);
        else if (val instanceof Double)
            assert Objects.equals(res.getDouble("value"), val);
        else if (val instanceof Float)
            assert Objects.equals(res.getFloat("value"), val);
        else if (val instanceof BigDecimal)
            assert Objects.equals(res.getBigDecimal("value"), val);
        else if (val instanceof java.sql.Timestamp )
            assert Objects.equals(res.getTimestamp("value"), val);
        else if (val instanceof LocalDateTime)
            assert Objects.equals(res.getTimestamp("value"), Timestamp.valueOf((LocalDateTime)val));
        else if (val.getClass() == java.util.Date.class)
            assert Objects.equals(res.getTimestamp("value").getTime(), ((java.util.Date)val).getTime());
        else if (val instanceof Period)
            assert Objects.equals(actVal, formatPeriod((Period)val));
        else if (val instanceof Duration)
            assert Objects.equals(res.getBigDecimal("value"), toBigDecimal((Duration)val));
        else if (val instanceof OffsetDateTime)
            assert Objects.equals(res.getObject("value", OffsetDateTime.class).withOffsetSameInstant(ZoneOffset.UTC), ((OffsetDateTime)val).withOffsetSameInstant(ZoneOffset.UTC));
        else if (val instanceof ZonedDateTime)
            assert Objects.equals(res.getObject("value", ZonedDateTime.class), val);
        else if (val instanceof byte[])
            assert Arrays.equals(res.getBytes("value"), (byte[])val);
        else
            assert Objects.equals(actVal, val.toString());
    }

    /** */
    private void checkType(Object val, String actTypeName) {
        if (val instanceof Byte || val instanceof Short)
            assert Objects.equals(actTypeName, "int2");
        else if (val instanceof Integer)
            assert Objects.equals(actTypeName, "int4");
        else if (val instanceof Long)
            assert Objects.equals(actTypeName, "int8");
        else if (val instanceof OffsetDateTime)
            assert Objects.equals(actTypeName, "timestamptz");
        else
            assert Objects.equals(actTypeName, JavaToSqlTypeMapper.renderSqlType(val.getClass().getName()).toLowerCase());
    }

    /** */
    private void checkValueMeta(ResultSetMetaData meta, Object val, Integer precision, Integer scale) throws SQLException {
        int actPrec = meta.getPrecision(2);
        int actScale = meta.getScale(2);

        if (val instanceof java.sql.Time || val instanceof LocalTime)
            assert Objects.equals(actPrec, precision + 9);
        else if (val instanceof java.sql.Timestamp || val instanceof LocalDateTime || val.getClass() == java.util.Date.class)
            assert Objects.equals(actPrec, precision + 23);
        else
            assert Objects.equals(actPrec, precision);

        if (val instanceof java.sql.Time || val instanceof java.sql.Timestamp || val instanceof LocalTime || val instanceof LocalDateTime ||  val.getClass() == java.util.Date.class)
            assert Objects.equals(actScale, precision);
        else
            assert Objects.equals(actScale, scale);
    }

    /** */
    private String formatPeriod(Period p) {
        StringBuilder sb = new StringBuilder();

        if (p.getYears() != 0)
            sb.append(p.getYears()).append(" year ");

        if (p.getMonths() != 0)
            sb.append(p.getMonths()).append(" mons ");

        if (p.getDays() != 0)
            sb.append(p.getDays()).append(" days");

        return sb.toString().trim();
    }

    /**
     *
     */
    private <V> IgniteCache<Integer, V> createCache(IgniteEx src, String tableName, String clsName, Integer precision, Integer scale) {
        QueryEntity qryEntity = new QueryEntity()
            .setTableName(tableName)
            .setKeyFieldName("id")
            .setValueFieldName("value")
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("value", clsName, null);

        if (precision != null)
            qryEntity.setFieldsPrecision(Map.of("value", precision));

        if (scale != null)
            qryEntity.setFieldsScale(Map.of("value", scale));

        CacheConfiguration<Integer, V> ccfg = new CacheConfiguration<Integer, V>(qryEntity.getTableName())
            .setQueryEntities(Collections.singletonList(qryEntity));

        return src.getOrCreateCache(ccfg);
    }

    /** */
    public enum NumericMeta {
        /** */
        NO_NUMERIC_META,

        /** */
        PRECISION_ONLY,

        /** */
        PRECISION_AND_SCALE,
    }
}
