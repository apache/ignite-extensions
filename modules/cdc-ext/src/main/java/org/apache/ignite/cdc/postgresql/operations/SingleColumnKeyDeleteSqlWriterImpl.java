package org.apache.ignite.cdc.postgresql.operations;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cdc.CdcEvent;

import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.getFirstPrimaryKey;

/**
 * This class is responsible for generating SQL DELETE statements for a single row based on a primary key.
 *
 * <h3>Example:</h3>
 * Suppose we have a table named "tableName" with a primary key "id".
 * <pre>
 * DELETE FROM tableName WHERE id IN (key1, key2, key3 ...)
 * </pre>
 *
 * @see AbstractSqlWriter
 * @see CdcEvent
 */
public class SingleColumnKeyDeleteSqlWriterImpl extends AbstractSqlWriter {
    /**
     * @param entity     The {@link QueryEntity} representing the table and its metadata.
     * @param sqlValSize The maximum size of the SQL value string.
     */
    public SingleColumnKeyDeleteSqlWriterImpl(QueryEntity entity, long sqlValSize) {
        super(entity, sqlValSize);
    }

    /** {@inheritDoc} */
    @Override protected String prepareSqlTemplate(QueryEntity entity) {
        return "DELETE FROM " + entity.getTableName() + " WHERE " + getFirstPrimaryKey(entity) + " IN (%s)";
    }

    /** {@inheritDoc} */
    @Override protected String prepareDelimiter() {
        return ", ";
    }

    /** {@inheritDoc} */
    @Override public void addEvent(CdcEvent evt, StringBuilder valSql) {
        valSql.append(evt.key());
    }
}
