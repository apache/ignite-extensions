package org.apache.ignite.cdc.postgresql.operations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cdc.CdcEvent;

/**
 * Abstract class that implements the replication logic for the Ignite-to-PostgreSQL connector.
 */
public abstract class AbstractSqlWriter {
    /** Number of rows included in one SQL statement. */
    private final long sqlValSize;

    /** Reusable template for generating SQL queries. */
    private final String sqlTemplate;

    /** Delimiter that separates CdcEvents within a single SQL statement. */
    private final String delim;

    /** StringBuilder for accumulating CdcEvents for SQL statement. */
    private StringBuilder valSql;

    /** Counter for the number of values added to the SQL statement. */
    private int valCnt;

    /**
     * Constructor for AbstractSqlWriter.
     *
     * @param entity QueryEntity object.
     * @param sqlValSize Number of rows included in one SQL statement.
     */

    public AbstractSqlWriter(QueryEntity entity, long sqlValSize) {
        this.sqlValSize = sqlValSize;
        this.sqlTemplate = prepareSqlTemplate(entity);
        this.delim = prepareDelimiter();
    }

    /**
     * @param entity QueryEntity object.
     * @return Prepared SQL query template.
     */
    protected abstract String prepareSqlTemplate(QueryEntity entity);

    /**
     * Prepares the delimiter for separating CdcEvents within a single SQL statement.
     *
     * @return Prepared delimiter.
     */
    protected abstract String prepareDelimiter();

    /**
     * Adds a CdcEvent to the SQL statement.
     *
     * @param evt CdcEvent object.
     * @return True if the number of values added to the SQL statement is greater than or equal to sqlValSize, false otherwise.
     * Used as a trigger for SQL batch formation.
     */
    public final boolean addEvent(CdcEvent evt) {
        if (valSql == null) {
            valSql = new StringBuilder();
            valCnt = 0;
        }

        if (valCnt != 0)
            valSql.append(delim);

        addEvent(evt, valSql);

        valCnt++;

        return valCnt >= sqlValSize;
    }

    /**
     * Adds a CdcEvent to StringBuilder for accumulating CdcEvents for SQL statement.
     *
     * @param evt CdcEvent object.
     * @param valSql StringBuilder for accumulating values for SQL statement.
     */
    public abstract void addEvent(CdcEvent evt, StringBuilder valSql);

    /**
     * Adds a batch of SQL statements to the connection.
     *
     * @param conn Connection object.
     * @param batch List of PreparedStatement objects.
     * @return Number of SQL statements added to the batch.
     * @throws SQLException if a database access error occurs.
     */
    public final byte addBatch(Connection conn, List<PreparedStatement> batch) throws SQLException {
        if (valSql == null)
            return 0;

        PreparedStatement ps = conn.prepareStatement(String.format(sqlTemplate, valSql));

        ps.addBatch();
        batch.add(ps);

        valSql = null;

        return 1;
    }
}
