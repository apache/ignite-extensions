package org.apache.ignite.cdc.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.postgresql.operations.AbstractSqlWriter;
import org.apache.ignite.cdc.postgresql.operations.CreateSqlWriter;
import org.apache.ignite.cdc.postgresql.operations.MultiColumnKeyDeleteSqlWriterImpl;
import org.apache.ignite.cdc.postgresql.operations.SingleColumnKeyDeleteSqlWriterImpl;
import org.apache.ignite.cdc.postgresql.operations.UpsertSqlWriterImpl;

import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcApplier.SqlOperation.DELETE;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcApplier.SqlOperation.UPSERT;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.isSingleKey;

/**
 * The {@code IgniteToPostgreSqlCdcApplier} class handles Change Data Capture (CDC) operations between Apache Ignite
 * and a PostgreSQL database by generating appropriate SQL queries for synchronization purposes.
 *
 * <p>This class supports two types of SQL operations: INSERT/UPDATE (UPSERT) and DELETE records through the use of batched
 * prepared statements for improved performance.</p>
 *
 * <p>It creates necessary tables in PostgreSQL if they do not exist yet and processes change events
 * by preparing corresponding SQL statements.</p>
 */
public class IgniteToPostgreSqlCdcApplier {
    /** */
    private final CreateSqlWriter createSqlWriter;

    /** */
    private final AbstractSqlWriter upsertSqlWriter;

    /** */
    private final AbstractSqlWriter deleteSqlWriter;

    /** */
    private SqlOperation lastOp = UPSERT;

    /**
     * @param entity       Metadata object representing the schema of the target table
     * @param sqlValSize   Maximum allowed value size per record
     */
    public IgniteToPostgreSqlCdcApplier(QueryEntity entity, long sqlValSize) {
        this.createSqlWriter = new CreateSqlWriter(entity);

        this.upsertSqlWriter = new UpsertSqlWriterImpl(entity, sqlValSize);

        this.deleteSqlWriter = isSingleKey(entity) ?
            new SingleColumnKeyDeleteSqlWriterImpl(entity, sqlValSize) :
            new MultiColumnKeyDeleteSqlWriterImpl(entity, sqlValSize);
    }

    /**
     * @param conn Active connection to the PostgreSQL database
     * @throws SQLException If an exception occurs while executing the SQL creation command
     */
    public void createTableIfNotExists(Connection conn) throws SQLException {
        createSqlWriter.createTableIfNotExists(conn);
    }

    /**
     * @param evt  CdcEvent
     * @param conn Current active connection to the database
     * @param batch Collection of prepared statements ready for execution
     * @return Number of added SQL statements
     * @throws SQLException If any issues occur during processing or when submitting the batch
     */
    public byte handleEvent(CdcEvent evt, Connection conn, List<PreparedStatement> batch) throws SQLException {
        byte added = 0;

        SqlOperation curOp = SqlOperation.of(evt);

        if (lastOp != curOp) {
            added += addBatch(conn, batch, lastOp);

            lastOp = curOp;
        }

        boolean isReady = addEvent(evt, curOp);

        if (isReady)
            added += addBatch(conn, batch, curOp);

        return added;
    }

    /**
     * @param evt CdcEvent
     * @param op  Current SQL operation type (INSERT/UPDATE or DELETE)
     * @return True if the maximum limit of the SQL statement has been reached and it's ready for execution
     */
    private boolean addEvent(CdcEvent evt, SqlOperation op) {
        if (op == UPSERT)
            return upsertSqlWriter.addEvent(evt);

        if (op == DELETE)
            return deleteSqlWriter.addEvent(evt);

        throw new IgniteException("SQL operation is unknown [op=" + op + ']');
    }

    /**
     * Submits all collected SQL statements within a batch to the database.
     *
     * @param conn Active connection to PostgreSQL
     * @param batch Collection of prepared SQL statements
     * @param op Type of SQL operation being performed
     * @return Number of successfully processed SQL statements
     * @throws SQLException If there are problems during submission
     */
    private byte addBatch(Connection conn, List<PreparedStatement> batch, SqlOperation op) throws SQLException {
        if (op == UPSERT)
            return upsertSqlWriter.addBatch(conn, batch);

        if (op == DELETE)
            return deleteSqlWriter.addBatch(conn, batch);

        throw new IgniteException("SQL operation is unknown [op=" + op + ']');
    }

    /** */
    enum SqlOperation {
        /** */
        UPSERT,

        /** */
        DELETE;

        /**
         * @param evt CdcEvent
         * @return Corresponding SQL operation type
         */
        public static SqlOperation of(CdcEvent evt) {
            return evt.value() == null ? DELETE : UPSERT;
        }
    }
}
