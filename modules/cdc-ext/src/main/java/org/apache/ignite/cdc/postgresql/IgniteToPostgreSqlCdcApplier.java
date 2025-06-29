package org.apache.ignite.cdc.postgresql;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcEvent;

import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.encodeVersion;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.getCreateTableSqlStatement;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.getDeleteSqlQry;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.getPrimaryKeys;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.getUpsertSqlQry;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UNDEFINED_CACHE_ID;

/** */
public class IgniteToPostgreSqlCdcApplier {
    /** */
    private final long maxBatchSize;

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

    /** */
    private PreparedStatement curPrepStmt;

    /**
     * @param maxBatchSize the maximum number of CDC events to include in a single batch
     * @param log the {@link IgniteLogger} instance used for logging CDC processing events
     */
    public IgniteToPostgreSqlCdcApplier(long maxBatchSize, IgniteLogger log) {
        this.maxBatchSize = maxBatchSize;
        this.log = log;
    }

    /**
     * @param conn the active JDBC {@link Connection} to the PostgreSQL database
     * @param evts an {@link Iterator} of {@link CdcEvent} objects to be applied
     * @return the total number of events successfully batched and executed
     */
    public long applyEvents(Connection conn, Iterator<CdcEvent> evts) {
        long evtsApplied = 0;

        int currCacheId = UNDEFINED_CACHE_ID;
        SqlOperation prevOp = SqlOperation.UNDEFINED;

        CdcEvent evt;

        while (evts.hasNext()) {
            evt = evts.next();

            if (log.isDebugEnabled())
                log.debug("Event received [evt=" + evt + ']');

            SqlOperation curOp = SqlOperation.of(evt);

            if (currCacheId != evt.cacheId() || curOp != prevOp) {
                evtsApplied += executeBatch();

                currCacheId = evt.cacheId();
                prevOp = curOp;

                prepareStatement(conn, evt);
            }

            if (curKeys.size() >= maxBatchSize || curKeys.contains(evt.key()))
                evtsApplied += executeBatch();

            addEvent(evt);
        }

        if (currCacheId != UNDEFINED_CACHE_ID)
            evtsApplied += executeBatch();

        return evtsApplied;
    }

    /** */
    private int executeBatch() {
        if (curPrepStmt == null)
            return 0;

        try {
            curKeys.clear();

            if (log.isDebugEnabled())
                log.debug("Applying batch " + curPrepStmt.toString());

            return curPrepStmt.executeBatch().length;
        }
        catch (SQLException e) {
            log.error(e.getMessage(), e);

            throw new IgniteException(e);
        }
    }

    /** */
    private void prepareStatement(Connection conn, CdcEvent evt) {
        String sqlQry;

        if (evt.value() == null)
            sqlQry = cacheIdToDeleteQry.get(evt.cacheId());
        else
            sqlQry = cacheIdToUpsertQry.get(evt.cacheId());

        if (log.isDebugEnabled())
            log.debug("Statement updated [cacheId=" + evt.cacheId() + ", sqlQry=" + sqlQry + ']');

        try {
            curPrepStmt = conn.prepareStatement(sqlQry);
        }
        catch (SQLException e) {
            log.error(e.getMessage(), e);

            throw new IgniteException(e);
        }
    }

    /** */
    private void addEvent(CdcEvent evt) {
        try {
            if (evt.value() == null)
                addEvent(evt, true);
            else {
                int idx = addEvent(evt, false);

                curPrepStmt.setBytes(idx, encodeVersion(evt.version()));
            }

            curPrepStmt.addBatch();
        }
        catch (Throwable e) {
            log.error(e.getMessage(), e);

            throw new IgniteException(e);
        }
    }

    /** */
    private int addEvent(CdcEvent evt, boolean isDelete) throws SQLException {
        Iterator<String> itFields = isDelete ?
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
                if (keyObj != null)
                    obj = keyObj.field(field);
                else
                    obj = evt.key();
            else
                if (valObj != null)
                    obj = valObj.field(field);
                else
                    obj = evt.value();

            addObject(idx, obj);

            idx++;
        }

        return idx; //curPrepStmt.setString(idx, evt.value().toString());
    }

    /**
     * Sets a value in the PreparedStatement at the given index using the appropriate setter
     * based on the runtime type of the object.
     */
    private void addObject(int idx, Object obj) throws SQLException {
        if (obj == null) {
            curPrepStmt.setObject(idx, null);

            return;
        }

        if (obj instanceof String)
            curPrepStmt.setString(idx, (String)obj);
        else if (obj instanceof Integer)
            curPrepStmt.setInt(idx, (Integer)obj);
        else if (obj instanceof Long)
            curPrepStmt.setLong(idx, (Long)obj);
        else if (obj instanceof Short)
            curPrepStmt.setShort(idx, (Short)obj);
        else if (obj instanceof Byte)
            curPrepStmt.setByte(idx, (Byte)obj);
        else if (obj instanceof Boolean)
            curPrepStmt.setBoolean(idx, (Boolean)obj);
        else if (obj instanceof Float)
            curPrepStmt.setFloat(idx, (Float)obj);
        else if (obj instanceof Double)
            curPrepStmt.setDouble(idx, (Double)obj);
        else if (obj instanceof BigDecimal)
            curPrepStmt.setBigDecimal(idx, (BigDecimal)obj);
        else if (obj instanceof UUID)
            curPrepStmt.setObject(idx, obj, java.sql.Types.OTHER); // PostgreSQL expects UUID as OTHER
        else if (obj instanceof byte[])
            curPrepStmt.setBytes(idx, (byte[])obj);
        else if (obj instanceof java.sql.Date)
            curPrepStmt.setDate(idx, (java.sql.Date)obj);
        else if (obj instanceof java.sql.Time)
            curPrepStmt.setTime(idx, (java.sql.Time)obj);
        else if (obj instanceof java.sql.Timestamp)
            curPrepStmt.setTimestamp(idx, (java.sql.Timestamp)obj);
        else if (obj instanceof java.util.Date)
            curPrepStmt.setTimestamp(idx, new java.sql.Timestamp(((java.util.Date)obj).getTime()));
        else if (obj instanceof java.time.LocalDate)
            curPrepStmt.setDate(idx, java.sql.Date.valueOf((java.time.LocalDate)obj));
        else if (obj instanceof java.time.LocalTime)
            curPrepStmt.setTime(idx, java.sql.Time.valueOf((java.time.LocalTime)obj));
        else if (obj instanceof java.time.LocalDateTime)
            curPrepStmt.setTimestamp(idx, java.sql.Timestamp.valueOf((java.time.LocalDateTime)obj));
        else if (obj instanceof java.time.OffsetDateTime)
            curPrepStmt.setTimestamp(idx, java.sql.Timestamp.from(((java.time.OffsetDateTime)obj).toInstant()));
        else if (obj instanceof java.time.ZonedDateTime)
            curPrepStmt.setTimestamp(idx, java.sql.Timestamp.from(((java.time.ZonedDateTime)obj).toInstant()));
        else
            curPrepStmt.setObject(idx, obj);
    }

    /**
     * @param conn the JDBC {@link Connection} to the PostgreSQL database
     * @param evts an {@link Iterator} of {@link CdcCacheEvent} objects to apply
     * @param createTables tables creation flag. If true - attempt to create tables will be made.
     */
    public void applyCacheEvents(Connection conn, Iterator<CdcCacheEvent> evts, boolean createTables) {
        CdcCacheEvent evt;
        QueryEntity entity;

        while (evts.hasNext()) {
            evt = evts.next();

            if (evt.queryEntities().size() != 1)
                throw new IgniteException("There should be exactly 1 QueryEntity for cacheId: " + evt.cacheId());

            entity = evt.queryEntities().iterator().next();

            if (createTables)
                createTableIfNotExists(conn, entity);

            cacheIdToUpsertQry.put(evt.cacheId(), getUpsertSqlQry(entity));

            cacheIdToDeleteQry.put(evt.cacheId(), getDeleteSqlQry(entity));

            cacheIdToPrimaryKeys.put(evt.cacheId(), getPrimaryKeys(entity));

            cacheIdToFields.put(evt.cacheId(), entity.getFields().keySet());
        }
    }

    /**
     * @param conn the JDBC {@link Connection} used to execute the DDL statement
     * @param entity the {@link QueryEntity} describing the table schema to create
     */
    private void createTableIfNotExists(Connection conn, QueryEntity entity) {
        String createSqlStmt = getCreateTableSqlStatement(entity);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createSqlStmt);
        }
        catch (SQLException e) {
            log.error(e.getMessage(), e);

            throw new IgniteException(e);
        }
    }

    /** */
    enum SqlOperation {
        /** */
        UPSERT,

        /** */
        DELETE,

        /** */
        UNDEFINED;

        /** */
        public static SqlOperation of(CdcEvent evt) {
            return evt.value() == null ? DELETE : UPSERT;
        }
    }
}
