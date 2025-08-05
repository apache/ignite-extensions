package org.apache.ignite.cdc.postgresql;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
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
import org.junit.Test;

import static org.apache.ignite.cdc.postgresql.JavaToSqlTypeMapperTest.NumericMeta.NO_NUMERIC_META;
import static org.apache.ignite.cdc.postgresql.JavaToSqlTypeMapperTest.NumericMeta.PRECISION_AND_SCALE;
import static org.apache.ignite.cdc.postgresql.JavaToSqlTypeMapperTest.NumericMeta.PRECISION_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class JavaToSqlTypeMapperTest extends CdcPostgreSqlReplicationAbstractTest {
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
        cfg.setConsistentId(igniteInstanceName);

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

        postgres = EmbeddedPostgres.builder().start();
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
        Set<String> cachesToReplicate = Arrays.stream(JavaToSqlTypeMapper.JavaToSqlType.values())
            .map(JavaToSqlTypeMapper.JavaToSqlType::javaTypeName)
            .filter(name -> !name.equals(Object.class.getName()))
            .map(clsName -> clsName.replace('.', '_').replace('[', '_'))
            .flatMap(clsName -> Stream.of(
                clsName,
                clsName + "_WithPrecision",
                clsName + "_WithPrecision_WithScale"
            ))
            .collect(Collectors.toSet());

        IgniteInternalFuture<?> fut = startIgniteToPostgreSqlCdcConsumer(
            src.configuration(),
            cachesToReplicate,
            postgres.getPostgresDatabase()
        );

        createCache("string", null, null);
        createCache("string", 10, null);
        createCache("string", 10, 10);

        createCache(5, null, null);
        createCache(5, 1, null);
        createCache(5, 1, 1);

        createCache(6L, null, null);
        createCache(6L, 2, null);
        createCache(6L, 2, 2);

        createCache(true, null, null);
        createCache(true, 3, null);
        createCache(true, 3, 3);

        createCache(23.0, null, null);
        createCache(23.0, 20, null);
        createCache(23.343, 10, 3);

        createCache(23.0f, null, null);
        createCache(23.0f, 10, null);
        createCache(23.12f, 5, 2);

        createCache(new BigDecimal("123.456"), null, null);
        createCache(new BigDecimal("1234"), 4, null);
        createCache(new BigDecimal("1.623"), 4, 3);

        createCache((short)23, null, null);
        createCache((short)23, 4, null);
        createCache((short)23, 4, 4);

        createCache((byte)33, null, null);
        createCache((byte)33, 5, null);
        createCache((byte)33, 5, 5);

        java.sql.Date sqlDate = new java.sql.Date(new java.util.Date().getTime());

        createCache(sqlDate, null, null);
        createCache(sqlDate, 6, null);
        createCache(sqlDate, 6, 6);

        java.sql.Time sqlTime = new java.sql.Time(933960600000L);

        createCache(sqlTime, null, null);
        createCache(sqlTime, 6, null);
        createCache(sqlTime, 6, 1);

        createCache(new java.sql.Timestamp(933960600000L), null, null);
        createCache(new java.sql.Timestamp(933960600000L), 2, null);
        createCache(new java.sql.Timestamp(933960612345L), 4, 1);

        createCache(new java.util.Date(933960600000L), null, null);
        createCache(new java.util.Date(933960600000L), 2, null);
        createCache(new java.util.Date(933960612345L), 4, 1);

        UUID uuid = UUID.randomUUID();

        createCache(uuid, null, null);
        createCache(uuid, 7, null);
        createCache(uuid, 7, 7);

        LocalDate locDate = LocalDate.of(1999, 8, 6);

        createCache(locDate, null, null);
        createCache(locDate, 10, null);
        createCache(locDate, 10, 10);

        LocalTime locTime = LocalTime.of(12, 12, 12);

        createCache(locTime, null, null);
        createCache(locTime, 6, null);
        createCache(locTime, 6, 1);

        LocalDateTime locDateTime = LocalDateTime.of(
            1999, 8, 6,
            23, 30, 3
        );

        createCache(locDateTime, null, null);
        createCache(locDateTime, 2, null);
        createCache(locDateTime, 4, 1);

        OffsetTime offsetTime = OffsetTime.now();

        createCache(offsetTime, null, null);
        createCache(offsetTime, 30, null);
        createCache(offsetTime, 30, 1);

        OffsetDateTime dateWithOffset = OffsetDateTime.of(locDateTime, ZoneOffset.ofHours(3));

        createCache(dateWithOffset, null, null);
        createCache(dateWithOffset, 11, null);
        createCache(dateWithOffset, 11, 11);

        createCache(new byte[]{1, 2, 3, 4}, null, null);
        createCache(new byte[]{1, 2, 3, 4}, 12, null);
        createCache(new byte[]{1, 2, 3, 4}, 12, 12);

        for (String cache : cachesToReplicate) {
            assertTrue(waitForCondition(waitForTablesCreatedOnPostgres(postgres, Set.of(cache)), getTestTimeout()));
            assertTrue(waitForCondition(waitForTableSize(postgres, cache, 1), getTestTimeout()));
        }

        checkCache("string", null, null, NO_NUMERIC_META);
        checkCache("string", 10, null, PRECISION_ONLY);
        checkCache("string", 10, 10, PRECISION_ONLY);

        checkCache(5, null, null, NO_NUMERIC_META);
        checkCache(5, 1, null, NO_NUMERIC_META);
        checkCache(5, 1, 1, NO_NUMERIC_META);

        checkCache(6L, null, null, NO_NUMERIC_META);
        checkCache(6L, 2, null, NO_NUMERIC_META);
        checkCache(6L, 2, 2, NO_NUMERIC_META);

        checkCache(true, null, null, NO_NUMERIC_META);
        checkCache(true, 3, null, NO_NUMERIC_META);
        checkCache(true, 3, 3, NO_NUMERIC_META);

        checkCache(23.0, null, null, NO_NUMERIC_META);
        checkCache(23.0, 20, null, PRECISION_ONLY);
        checkCache(23.343, 10, 3, PRECISION_AND_SCALE);

        checkCache(23.0f, null, null, NO_NUMERIC_META);
        checkCache(23.0f, 10, null, PRECISION_ONLY);
        checkCache(23.12f, 5, 2, PRECISION_AND_SCALE);

        checkCache(new BigDecimal("123.456"), null, null, NO_NUMERIC_META);
        checkCache(new BigDecimal("1234"), 4, null, PRECISION_ONLY);
        checkCache(new BigDecimal("1.623"), 4, 3, PRECISION_AND_SCALE);

        checkCache((short)23, null, null, NO_NUMERIC_META);
        checkCache((short)23, 4, null, NO_NUMERIC_META);
        checkCache((short)23, 4, 4, NO_NUMERIC_META);

        checkCache((byte)33, null, null, NO_NUMERIC_META);
        checkCache((byte)33, 5, null, NO_NUMERIC_META);
        checkCache((byte)33, 5, 5, NO_NUMERIC_META);

        checkCache(sqlDate, null, null, NO_NUMERIC_META);
        checkCache(sqlDate, 6, null, NO_NUMERIC_META);
        checkCache(sqlDate, 6, 6, NO_NUMERIC_META);

        checkCache(sqlTime, null, null, NO_NUMERIC_META);
        checkCache(sqlTime, 6, null, PRECISION_ONLY);
        checkCache(sqlTime, 6, 1, PRECISION_ONLY);

        checkCache(new java.sql.Timestamp(933960600000L), null, null, NO_NUMERIC_META);
        checkCache(new java.sql.Timestamp(933960600000L), 2, null, PRECISION_ONLY);
        checkCache(new java.sql.Timestamp(933960612345L), 4, 1, PRECISION_ONLY);

        checkCache(new java.util.Date(933960600000L), null, null, NO_NUMERIC_META);
        checkCache(new java.util.Date(933960600000L), 2, null, NO_NUMERIC_META);
        checkCache(new java.util.Date(933960612345L), 4, 1, NO_NUMERIC_META);

        checkCache(uuid, null, null, NO_NUMERIC_META);
        checkCache(uuid, 7, null, NO_NUMERIC_META);
        checkCache(uuid, 7, 7, NO_NUMERIC_META);

        checkCache(locDate, null, null, NO_NUMERIC_META);
        checkCache(locDate, 10, null, NO_NUMERIC_META);
        checkCache(locDate, 10, 10, NO_NUMERIC_META);

        checkCache(locTime, null, null, NO_NUMERIC_META);
        checkCache(locTime, 6, null, PRECISION_ONLY);
        checkCache(locTime, 6, 1, PRECISION_ONLY);

        checkCache(locDateTime, null, null, NO_NUMERIC_META);
        checkCache(locDateTime, 2, null, PRECISION_ONLY);
        checkCache(locDateTime, 4, 1, PRECISION_ONLY);

        checkCache(offsetTime, null, null, NO_NUMERIC_META);
        checkCache(offsetTime, 30, null, PRECISION_ONLY);
        checkCache(offsetTime, 30, 1, PRECISION_ONLY);

        checkCache(dateWithOffset, null, null, NO_NUMERIC_META);
        checkCache(dateWithOffset, 11, null, NO_NUMERIC_META);
        checkCache(dateWithOffset, 11, 11, NO_NUMERIC_META);

        checkCache(new byte[]{1, 2, 3, 4}, null, null, NO_NUMERIC_META);
        checkCache(new byte[]{1, 2, 3, 4}, 12, null, NO_NUMERIC_META);
        checkCache(new byte[]{1, 2, 3, 4}, 12, 12, NO_NUMERIC_META);

        fut.cancel();
    }

    /** */
    private <V> void createCache(V val, Integer precision, Integer scale) {
        String tableName = getTableNameFromClass(val.getClass(), precision, scale);

        IgniteCache<Integer, V> cache = createCache(src, tableName, val.getClass().getName(), precision, scale);

        cache.put(1, val);
    }

    /** */
    private <V> void checkCache(V val, Integer precision, Integer scale, NumericMeta actualMeta) {
        String tableName = getTableNameFromClass(val.getClass(), precision, scale);

        selectOnPostgreSqlAndAct(postgres, "SELECT * FROM " + tableName, (res) -> {
            assertTrue(res.next());

            checkValue(res, val);

            ResultSetMetaData meta = res.getMetaData();

            String actTypeName = meta.getColumnTypeName(2);

            checkType(val, actTypeName);

            if (actualMeta != NO_NUMERIC_META)
                checkValueMeta(meta, val, precision, actualMeta == PRECISION_AND_SCALE ? scale : 0);

            return true;
        });
    }

    /** */
    private String getTableNameFromClass(Class<?> cls, Integer precision, Integer scale) {
        return cls.getName().replace('.', '_').replace('[', '_') +
            (precision != null ? "_WithPrecision" : "") +
            (scale != null ? "_WithScale" : "");
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
        else if (val instanceof OffsetDateTime)
            assert Objects.equals(res.getObject("value", OffsetDateTime.class).withOffsetSameInstant(ZoneOffset.UTC),
                ((OffsetDateTime)val).withOffsetSameInstant(ZoneOffset.UTC));
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
            assert Objects.equals(actTypeName,
                javaToSqlTypeMapper.renderSqlType(val.getClass().getName(), null, null).toLowerCase());
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

        if (val instanceof java.sql.Time || val instanceof java.sql.Timestamp || val instanceof LocalTime ||
            val instanceof LocalDateTime || val.getClass() == java.util.Date.class)
            assert Objects.equals(actScale, precision);
        else
            assert Objects.equals(actScale, scale);
    }

    /** */
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
