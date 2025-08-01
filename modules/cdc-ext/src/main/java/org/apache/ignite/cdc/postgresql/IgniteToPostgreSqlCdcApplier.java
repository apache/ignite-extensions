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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UNDEFINED_CACHE_ID;

/** */
public class IgniteToPostgreSqlCdcApplier {
    /** */
    private final DataSource dataSrc;

    /** */
    private final long batchSize;

    /** */
    private final IgniteLogger log;

    /** */
    private final Map<Integer, String> cacheIdToUpsertQry = new HashMap<>();

    /** */
    private final Map<Integer, String> cacheIdToDeleteQry = new HashMap<>();

    /** */
    private final Map<Integer, Set<String>> cacheIdToPrimaryKeys = new HashMap<>();

    /** */
    private final Map<Integer, Set<String>> cacheIdToFields = new HashMap<>();

    /** */
    private final Set<Object> curKeys = new HashSet<>();

    /**
     * @param dataSrc {@link DataSource} - connection pool to PostgreSql
     * @param batchSize the number of CDC events to include in a single batch
     * @param log the {@link IgniteLogger} instance used for logging CDC processing events
     */
    public IgniteToPostgreSqlCdcApplier(
        DataSource dataSrc,
        long batchSize,
        IgniteLogger log
    ) {
        this.dataSrc = dataSrc;
        this.batchSize = batchSize;
        this.log = log;
    }

    /**
     * @param evts an {@link Iterator} of {@link CdcEvent} objects to be applied
     * @return the total number of events successfully batched and executed
     */
    public long applyEvents(Iterator<CdcEvent> evts) {
        try (Connection conn = dataSrc.getConnection()) {
            try {
                // Setting it to true doesn't make each SQL query commit individually - it still commits the entire batch.
                // We chose to handle commits manually for better control.
                conn.setAutoCommit(false);

                return applyEvents(conn, evts);
            }
            catch (SQLException e) {
                conn.rollback();

                throw e;
            }
        }
        catch (Throwable e) {
            log.error("Error during CDC event application: " + e.getMessage(), e);

            throw new IgniteException("Failed to apply CDC events", e);
        }
    }

    /**
     * @param conn connection to PostgreSql
     * @param evts an {@link Iterator} of {@link CdcEvent} objects to be applied
     * @return the total number of events successfully batched and executed
     */
    private long applyEvents(Connection conn, Iterator<CdcEvent> evts) {
        long evtsApplied = 0;

        int currCacheId = UNDEFINED_CACHE_ID;
        boolean prevOpIsDelete = false;
        
        PreparedStatement curPrepStmt = null;
        CdcEvent evt;

        while (evts.hasNext()) {
            evt = evts.next();

            if (log.isDebugEnabled())
                log.debug("Event received [evt=" + evt + ']');

            if (currCacheId != evt.cacheId() || prevOpIsDelete ^ (evt.value() == null)) {
                if (curPrepStmt != null)
                    evtsApplied += executeBatch(conn, curPrepStmt);

                currCacheId = evt.cacheId();
                prevOpIsDelete = evt.value() == null;

                curPrepStmt = prepareStatement(conn, evt);
            }

            if (curKeys.size() >= batchSize || curKeys.contains(evt.key()))
                evtsApplied += executeBatch(conn, curPrepStmt);

            addEvent(curPrepStmt, evt);
        }

        if (!curKeys.isEmpty())
            evtsApplied += executeBatch(conn, curPrepStmt);

        return evtsApplied;
    }

    /**
     * @param conn connection to PostgreSql
     * @param curPrepStmt {@link PreparedStatement}
     * @return the total number of batches successfully executed. One CdcEvent - one batch.
     */
    private int executeBatch(Connection conn, PreparedStatement curPrepStmt) {
        try {
            curKeys.clear();

            if (log.isDebugEnabled())
                log.debug("Applying batch " + curPrepStmt.toString());

            if (!curPrepStmt.isClosed()) {
                int batchSize = curPrepStmt.executeBatch().length;

                // It's better to use autoCommit = false and call commit() manually for improved performance and
                // clearer transaction boundaries
                conn.commit();

                return batchSize;
            }

            throw new IgniteException("Tried to execute on closed prepared statement!");
        }
        catch (SQLException e) {
            log.error(e.getMessage(), e);

            throw new IgniteException(e);
        }
    }

    /**
     * @param conn connection to PostgreSql
     * @param evt {@link CdcEvent}
     * @return relevant {@link PreparedStatement}
     */
    private PreparedStatement prepareStatement(Connection conn, CdcEvent evt) {
        String sqlQry;

        if (evt.value() == null)
            sqlQry = cacheIdToDeleteQry.get(evt.cacheId());
        else
            sqlQry = cacheIdToUpsertQry.get(evt.cacheId());

        if (sqlQry == null)
            throw new IgniteException("No SQL query is found for cacheId=" + evt.cacheId());

        if (log.isDebugEnabled())
            log.debug("Statement updated [cacheId=" + evt.cacheId() + ", sqlQry=" + sqlQry + ']');

        try {
            return conn.prepareStatement(sqlQry);
        }
        catch (SQLException e) {
            log.error(e.getMessage(), e);

            throw new IgniteException(e);
        }
    }

    /**
     * @param curPrepStmt current {@link PreparedStatement}
     * @param evt {@link CdcEvent}
     */
    private void addEvent(PreparedStatement curPrepStmt, CdcEvent evt) {
        try {
            Iterator<String> itFields = evt.value() == null ?
                cacheIdToPrimaryKeys.get(evt.cacheId()).iterator() :
                cacheIdToFields.get(evt.cacheId()).iterator();

            String field;

            BinaryObject keyObj = (evt.key() instanceof BinaryObject) ? (BinaryObject)evt.key() : null;
            BinaryObject valObj = (evt.value() instanceof BinaryObject) ? (BinaryObject)evt.value() : null;

            int idx = 1;
            Object obj;

            while (itFields.hasNext()) {
                field = itFields.next();

                if (cacheIdToPrimaryKeys.get(evt.cacheId()).contains(field))
                    obj = keyObj != null ? keyObj.field(field) : evt.key();
                else
                    obj = valObj != null ? valObj.field(field) : evt.value();

                JavaToSqlTypeMapper.setEventFieldValue(curPrepStmt, idx, obj);

                idx++;
            }

            if (evt.value() != null)
                curPrepStmt.setBytes(idx, encodeVersion(evt.version()));

            curKeys.add(evt.key());

            curPrepStmt.addBatch();
        }
        catch (Throwable e) {
            log.error(e.getMessage(), e);

            throw new IgniteException(e);
        }
    }

    /**
     * @param evts an {@link Iterator} of {@link CdcCacheEvent} objects to apply
     * @param createTables tables creation flag. If true - attempt to create tables will be made.
     * @return Number of applied events.
     */
    public long applyCacheEvents(Iterator<CdcCacheEvent> evts, boolean createTables) {
        CdcCacheEvent evt;
        QueryEntity entity;

        long cnt = 0;

        while (evts.hasNext()) {
            evt = evts.next();

            if (evt.queryEntities().size() != 1)
                throw new IgniteException("There should be exactly 1 QueryEntity for cacheId: " + evt.cacheId());

            entity = evt.queryEntities().iterator().next();

            if (createTables)
                createTableIfNotExists(entity);

            cacheIdToUpsertQry.put(evt.cacheId(), getUpsertSqlQry(entity));

            cacheIdToDeleteQry.put(evt.cacheId(), getDeleteSqlQry(entity));

            cacheIdToPrimaryKeys.put(evt.cacheId(), getPrimaryKeys(entity));

            cacheIdToFields.put(evt.cacheId(), entity.getFields().keySet());

            if (createTables && log.isInfoEnabled())
                log.info("Cache table created [tableName=" + entity.getTableName() +
                    ", columns=" + entity.getFields().keySet() + ']');

            cnt++;
        }

        return cnt;
    }

    /**
     * @param entity the {@link QueryEntity} describing the table schema to create
     */
    private void createTableIfNotExists(QueryEntity entity) {
        String createSqlStmt = getCreateTableSqlStatement(entity);

        try (Connection conn = dataSrc.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(createSqlStmt);
        }
        catch (SQLException e) {
            log.error(e.getMessage(), e);

            throw new IgniteException(e);
        }
    }

    /**
     * Generates the SQL statement for creating a table.
     *
     * @param entity QueryEntity instance describing the cache structure.
     * @return SQL statement for creating a table.
     */
    private String getCreateTableSqlStatement(QueryEntity entity) {
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
    private void addFieldsAndTypes(QueryEntity entity, StringBuilder sql) {
        Iterator<Map.Entry<String, String>> iter = entity.getFields().entrySet().iterator();

        Map.Entry<String, String> field;
        String type;

        Integer precision;
        Integer scale;

        while (iter.hasNext()) {
            field = iter.next();

            precision = entity.getFieldsPrecision().get(field.getKey());
            scale = entity.getFieldsScale().get(field.getKey());

            if (precision != null && scale != null)
                type = JavaToSqlTypeMapper.renderSqlType(field.getValue(), precision, scale);
            else if (precision != null)
                type = JavaToSqlTypeMapper.renderSqlType(field.getValue(), precision);
            else
                type = JavaToSqlTypeMapper.renderSqlType(field.getValue());

            sql.append(field.getKey()).append(" ").append(type);

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
     *   <li>All fields are assigned {@code ?} placeholders for use with {@link PreparedStatement}.</li>
     * </ul>
     *
     * @param entity the {@link QueryEntity} describing the table, fields, and primary keys
     * @return a SQL UPSERT query string with parameter placeholders and version conflict resolution
     */
    private String getUpsertSqlQry(QueryEntity entity) {
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

        sql.append(" WHERE ").append(entity.getTableName()).append(".version < EXCLUDED.version");

        return sql.toString();
    }

    /**
     * Builds a comma-separated list of field names extracted from the QueryEntity.
     *
     * @param entity QueryEntity instance describing the cache structure.
     * @param sql Target StringBuilder where the result will be appended.
     */
    private void addFields(QueryEntity entity, StringBuilder sql) {
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
    private void addUpdateFields(QueryEntity entity, StringBuilder sql) {
        Set<String> primaryFields = getPrimaryKeys(entity);

        Iterator<String> itAllFields = F.concat(false, "version", entity.getFields().keySet()).iterator();

        String field;

        boolean first = true;

        while (itAllFields.hasNext()) {
            field = itAllFields.next();

            if (primaryFields.contains(field))
                continue;

            if (!first)
                sql.append(", ");

            sql.append(field).append(" = EXCLUDED.").append(field);

            if (first)
                first = false;
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
    private String getDeleteSqlQry(QueryEntity entity) {
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
    private void addPrimaryKeys(QueryEntity entity, StringBuilder sql) {
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
    private Set<String> getPrimaryKeys(QueryEntity entity) {
        Set<String> keys = entity.getKeyFields();

        if (keys == null || keys.isEmpty()) {
            if (entity.getKeyFieldName() == null)
                throw new IgniteException("Couldn't determine key field for queryEntity [tableName=" +
                    entity.getTableName() + ']');

            return Collections.singleton(entity.getKeyFieldName());
        }

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
    private byte[] encodeVersion(CacheEntryVersion ver) {
        ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);

        buf.putInt(ver.topologyVersion());
        buf.putLong(ver.order());
        buf.putInt(ver.nodeOrder());

        return buf.array();
    }
}
