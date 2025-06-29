package org.apache.ignite.cdc.postgresql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Utility class for managing conversion of Java types to PostgreSQL SQL types and related helper functionality
 * specifically tailored for working with Apache Ignite-to-PostgreSQL CDC (Change Data Capture) replication tasks.
 *
 * <p>
 *     Contains predefined mappings of common Java types to their equivalent SQL representations and utility methods
 *     for extracting key fields, building SQL queries, and other relevant operations needed for CDC replication.
 * </p>
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

    /**
     * Generates the SQL statement for creating a table.
     *
     * @param entity QueryEntity instance describing the cache structure.
     * @return SQL statement for creating a table.
     */
    public static String getCreateTableSqlStatement(QueryEntity entity) {
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(entity.getTableName()).append(" (");

        addFieldsAndTypes(entity, ddl);

        ddl.append(", version BYTEA NOT NULL");

        ddl.append(", PRIMARY KEY (");

        addPrimaryKeys(entity, ddl);

        ddl.append(')').append(')');

        return ddl.toString();
    }

    /**
     * Constructs DDL-compatible SQL fragment listing fields along with their mapped SQL types.
     *
     * @param entity QueryEntity instance describing the cache structure.
     * @param sql Target StringBuilder where the result will be appended.
     */
    private static void addFieldsAndTypes(QueryEntity entity, StringBuilder sql) {
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
     * Generates a parameterized SQL UPSERT (INSERT ... ON CONFLICT DO UPDATE) query
     * for the given {@link QueryEntity}, including a version-based conflict resolution condition.
     * <pre>{@code
     * INSERT INTO my_table (id, name, version) VALUES (?, ?, ?)
     * ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
     * WHERE version < EXCLUDED.version
     * }</pre>
     *
     * Notes:
     * <ul>
     *   <li>The {@code version} field is added to support version-based upsert logic.</li>
     *   <li>Primary key fields are excluded from the {@code DO UPDATE SET} clause.</li>
     *   <li>All fields are assigned {@code ?} placeholders for use with {@link java.sql.PreparedStatement}.</li>
     * </ul>
     *
     * @param entity the {@link QueryEntity} describing the table, fields, and primary keys
     * @return a SQL UPSERT query string with parameter placeholders and version conflict resolution
     */
    public static String getUpsertSqlQry(QueryEntity entity) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(entity.getTableName()).append(" (");

        addFields(entity, sql);

        sql.append(", version) VALUES (");

        for (int i = 0; i < entity.getFields().size() + 1; ++i) { // version field included
            sql.append('?');

            if (i < entity.getFields().size())
                sql.append(", ");
        }

        sql.append(") ON CONFLICT (");

        addPrimaryKeys(entity, sql);

        sql.append(") DO UPDATE SET ");

        addUpdateFields(entity, sql);

        sql.append(" WHERE version < EXCLUDED.version");

        return sql.toString();
    }

    /**
     * Builds a comma-separated list of field names extracted from the QueryEntity.
     *
     * @param entity QueryEntity instance describing the cache structure.
     * @param sql Target StringBuilder where the result will be appended.
     */
    private static void addFields(QueryEntity entity, StringBuilder sql) {
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
     * Builds a SQL update clause excluding primary key fields, including version-specific fields.
     *
     * @param entity QueryEntity instance describing the cache structure.
     * @param sql Target StringBuilder where the resulting SQL fragment will be appended.
     */
    private static void addUpdateFields(QueryEntity entity, StringBuilder sql) {
        Set<String> primaryFields = getPrimaryKeys(entity);

        Iterator<String> itAllFields = F.concat(false, "version", entity.getFields().keySet()).iterator();

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
     * Generates a parameterized SQL DELETE query for the given {@link QueryEntity}.
     * Example:
     * <pre>{@code
     * // For a key: id
     * DELETE FROM my_table WHERE (id = ?)
     * }</pre>
     *
     * If the table has a composite primary key, all keys will be included with AND conditions:
     * <pre>{@code
     * // For a composite key: id1, id2
     * DELETE FROM my_table WHERE (id1 = ? AND id2 = ?)
     * }</pre>
     *
     * @param entity the {@link QueryEntity} describing the table and its primary keys
     * @return a SQL DELETE query string with parameter placeholders for primary key values
     */
    public static String getDeleteSqlQry(QueryEntity entity) {
        StringBuilder deleteQry = new StringBuilder("DELETE FROM ").append(entity.getTableName()).append(" WHERE (");

        Iterator<String> itKeys = getPrimaryKeys(entity).iterator();
        String key;

        while (itKeys.hasNext()) {
            key = itKeys.next();

            deleteQry.append(key).append(" = ?");

            if (itKeys.hasNext())
                deleteQry.append(" AND ");
        }

        deleteQry.append(')');

        return deleteQry.toString();
    }

    /**
     * Generates a SQL fragment listing primary key fields for the given QueryEntity.
     *
     * @param entity QueryEntity instance describing the cache structure.
     * @param sql Target StringBuilder where the resulting SQL fragment will be appended.
     */
    private static void addPrimaryKeys(QueryEntity entity, StringBuilder sql) {
        Iterator<String> iterKeys = getPrimaryKeys(entity).iterator();

        while (iterKeys.hasNext()) {
            sql.append(iterKeys.next());

            if (iterKeys.hasNext())
                sql.append(", ");
        }
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
     * Encodes the components of a {@link CacheEntryVersion} into a 16-byte array
     * using big-endian byte order for compact and lexicographically comparable storage.
     * <p>
     * The encoding format is:
     * <ul>
     *   <li>4 bytes — {@code topologyVersion} (int)</li>
     *   <li>8 bytes — {@code order} (long)</li>
     *   <li>4 bytes — {@code nodeOrder} (int)</li>
     * </ul>
     * This format ensures that the resulting {@code byte[]} can be compared
     * lexicographically to determine version ordering (i.e., a larger byte array
     * represents a newer version), which is compatible with PostgreSQL {@code BYTEA}
     * comparison semantics.
     *
     * @param ver the {@link CacheEntryVersion} instance to encode
     * @return a 16-byte array representing the version in big-endian format
     */
    public static byte[] encodeVersion(CacheEntryVersion ver) {
        ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);

        buf.putInt(ver.topologyVersion());
        buf.putLong(ver.order());
        buf.putInt(ver.nodeOrder());

        return buf.array();
    }
}
