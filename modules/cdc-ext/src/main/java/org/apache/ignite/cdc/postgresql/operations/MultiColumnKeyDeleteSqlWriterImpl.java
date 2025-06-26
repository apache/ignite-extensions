package org.apache.ignite.cdc.postgresql.operations;

import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cdc.CdcEvent;

/**
 * This class is responsible for generating SQL DELETE statements for multiple rows based on primary keys.
 *
 * <h3>Example:</h3>
 * Suppose we have a table named "tableName" with primary keys "key1" and "key2".
 * <pre>
 * DELETE FROM tableName WHERE (key1 = X1 AND key2 = Y1) OR (key1 = X2 AND key2 = Y2) OR (key1 = X3 AND key2 = Y3) ...
 * </pre>
 *
 * @see AbstractSqlWriter
 * @see CdcEvent
 */
public class MultiColumnKeyDeleteSqlWriterImpl extends AbstractSqlWriter {
    /** */
    private final Set<String> primaryKeys;

    /**
     * @param entity     The {@link QueryEntity} representing the table and its metadata.
     * @param sqlValSize The maximum size of the SQL value string.
     */
    public MultiColumnKeyDeleteSqlWriterImpl(QueryEntity entity, long sqlValSize) {
        super(entity, sqlValSize);

        this.primaryKeys = entity.getKeyFields();
    }

    /** {@inheritDoc} */
    @Override protected String prepareSqlTemplate(QueryEntity entity) {
        return "DELETE FROM " + entity.getTableName() + " WHERE %s";
    }

    /** {@inheritDoc} */
    @Override protected String prepareDelimiter() {
        return " OR ";
    }

    /** {@inheritDoc} */
    @Override public void addEvent(CdcEvent evt, StringBuilder valSql) {
        valSql.append('(');

        Iterator<String> itKeys = primaryKeys.iterator();
        String key;

        while (itKeys.hasNext()) {
            key = itKeys.next();

            valSql.append(key).append(" = ").append(((BinaryObject)evt.key()).field(key).toString());

            if (itKeys.hasNext())
                valSql.append(" AND ");
        }

        valSql.append(')');
    }
}


