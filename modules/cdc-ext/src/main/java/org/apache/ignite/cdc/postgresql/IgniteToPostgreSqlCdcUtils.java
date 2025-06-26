package org.apache.ignite.cdc.postgresql;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Utility class for managing conversion of Java types to PostgreSQL SQL types and related helper functionality
 * specifically tailored for working with Apache Ignite-to-PostgreSQL CDC (Change Data Capture) replication tasks.
 *
 * <p>Contains predefined mappings of common Java types to their equivalent SQL representations and utility methods
 * for extracting key fields, building SQL queries, and other relevant operations needed for CDC replication.</p>
 */
public final class IgniteToPostgreSqlCdcUtils {
    /** */
    public static final String DFLT_SQL_TYPE = "BYTEA";

    /** */
    public static final Map<String, String> JAVA_TO_SQL_TYPES = Map.ofEntries(
        Map.entry("java.lang.String", "TEXT"),
        Map.entry("java.lang.Integer", "INT"),
        Map.entry("int", "INT"),
        Map.entry("java.lang.Long", "BIGINT"),
        Map.entry("long", "BIGINT"),
        Map.entry("java.lang.Boolean", "BOOLEAN"),
        Map.entry("boolean", "BOOLEAN"),
        Map.entry("java.lang.Double", "DOUBLE PRECISION"),
        Map.entry("double", "DOUBLE PRECISION"),
        Map.entry("java.lang.Float", "REAL"),
        Map.entry("float", "REAL"),
        Map.entry("java.math.BigDecimal", "NUMERIC"),
        Map.entry("java.lang.Short", "SMALLINT"),
        Map.entry("short", "SMALLINT"),
        Map.entry("java.lang.Byte", "SMALLINT"),
        Map.entry("byte", "SMALLINT"),
        Map.entry("java.util.Date", "DATE"),
        Map.entry("java.time.LocalDate", "DATE"),
        Map.entry("java.sql.Timestamp", "TIMESTAMP"),
        Map.entry("java.time.LocalDateTime", "TIMESTAMP"),
        Map.entry("java.util.UUID", "UUID"),
        Map.entry("[B", "BYTEA")
    );

    /** */
    public static final Map<String, String> VERSION_FIELDS;

    static {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("version_topology", "INT");
        map.put("version_order", "BIGINT");
        map.put("version_node_order", "INT");

        VERSION_FIELDS = Collections.unmodifiableMap(map);
    }

    /**
     * @param entity The {@link QueryEntity} representing the table and its metadata.
     * @return {@code true} if there is a single primary key, otherwise {@code false}.
     */
    public static boolean isSingleKey(QueryEntity entity) {
        Set<String> keys = entity.getKeyFields();

        return keys == null || keys.isEmpty() || keys.size() == 1;
    }

    /**
     * Retrieves the first primary key field name from the provided {@link QueryEntity}.
     * If no primary keys are defined, it returns the first field from the table.
     *
     * @param entity The {@link QueryEntity} representing the table and its metadata.
     * @return The name of the first primary key field or the first field if no primary keys are defined.
     */
    public static String getFirstPrimaryKey(QueryEntity entity) {
        Set<String> keys = entity.getKeyFields();

        if (keys == null || keys.isEmpty())
            return entity.getFields().keySet().iterator().next();

        return keys.stream().findFirst().orElseThrow();
    }

    /**
     * Retrieves the primary key field names from the provided {@link QueryEntity}.
     * If no primary keys are defined, it returns a set containing the first field from the table.
     *
     * @param entity The {@link QueryEntity} representing the table and its metadata.
     * @return A set of primary key field names or a set containing the first field if no primary keys are defined.
     */
    public static Set<String> getPrimaryKeys(QueryEntity entity) {
        Set<String> keys = entity.getKeyFields();

        if (keys == null || keys.isEmpty())
            return Set.of(entity.getFields().keySet().iterator().next());

        return keys;
    }

    /**
     * Builds a comma-separated list of field names extracted from the QueryEntity.
     *
     * @param entity QueryEntity instance describing the cache structure.
     * @param sql Target StringBuilder where the result will be appended.
     */
    public static void addFields(QueryEntity entity, StringBuilder sql) {
        Iterator<Map.Entry<String, String>> iter = entity.getFields().entrySet().iterator();
        Map.Entry<String, String> field;

        while (iter.hasNext()) {
            field = iter.next();

            sql.append(field.getKey());

            if (iter.hasNext())
                sql.append(", ");
        }
    }

    /**
     * Constructs DDL-compatible SQL fragment listing fields along with their mapped SQL types.
     *
     * @param entity QueryEntity instance describing the cache structure.
     * @param sql Target StringBuilder where the result will be appended.
     */
    public static void addFieldsForDdl(QueryEntity entity, StringBuilder sql) {
        Iterator<Map.Entry<String, String>> iter = entity.getFields().entrySet().iterator();
        Map.Entry<String, String> field;

        while (iter.hasNext()) {
            field = iter.next();

            sql.append(field.getKey()).append(" ").append(JAVA_TO_SQL_TYPES.getOrDefault(field.getValue(), DFLT_SQL_TYPE));

            if (iter.hasNext())
                sql.append(", ");
        }
    }

    /**
     * Appends version-related columns to the given SQL StringBuilder for DDL purposes.
     *
     * @param sql Target StringBuilder where the version fields will be appended.
     */
    public static void addVersionFields(StringBuilder sql) {
        Iterator<String> itIdxFileds = VERSION_FIELDS.keySet().iterator();

        while (itIdxFileds.hasNext()) {
            sql.append(itIdxFileds.next());

            if (itIdxFileds.hasNext())
                sql.append(", ");
        }
    }

    /**
     * Generates a SQL fragment listing primary key fields for the given QueryEntity.
     *
     * @param entity QueryEntity instance describing the cache structure.
     * @param sql Target StringBuilder where the resulting SQL fragment will be appended.
     */
    public static void addPrimaryKeys(QueryEntity entity, StringBuilder sql) {
        String key;

        if (isSingleKey(entity))
            sql.append(getFirstPrimaryKey(entity));
        else {
            Iterator<String> iterKeys = entity.getKeyFields().iterator();

            while (iterKeys.hasNext()) {
                key = iterKeys.next();

                sql.append(key);

                if (iterKeys.hasNext())
                    sql.append(", ");
            }
        }
    }

    /**
     * Builds a SQL update clause excluding primary key fields, including version-specific fields.
     *
     * @param entity QueryEntity instance describing the cache structure.
     * @param sql Target StringBuilder where the resulting SQL fragment will be appended.
     */
    public static void addUpdateFields(QueryEntity entity, StringBuilder sql) {
        Set<String> primaryFields = getPrimaryKeys(entity);

        Iterator<String> itFields = entity.getFields().keySet().stream().iterator();
        Iterator<String> itAllFields = F.concat(itFields, VERSION_FIELDS.keySet().iterator());

        String field;

        while (itAllFields.hasNext()) {
            field = itAllFields.next();

            if (primaryFields.contains(field))
                continue;

            sql.append(field).append(" = EXCLUDED.").append(field);

            if (itAllFields.hasNext())
                sql.append(", ");
        }
    }

    /**
     * Appends a version comparison condition to the SQL fragment suitable for ON CONFLICT clauses.
     *
     * @param sql Target StringBuilder where the resulting SQL fragment will be appended.
     */
    public static void addVersionComparisonClause(StringBuilder sql) {
        sql.append('(');

        addVersionFields(sql);

        sql.append(") <= (");

        Iterator<String> itIdxFileds = VERSION_FIELDS.keySet().iterator();

        while (itIdxFileds.hasNext()) {
            sql.append("EXCLUDED.").append(itIdxFileds.next());

            if (itIdxFileds.hasNext())
                sql.append(", ");
        }

        sql.append(')');
    }
}
