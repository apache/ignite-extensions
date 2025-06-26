package org.apache.ignite.cdc.postgresql.operations;

import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cdc.CdcEvent;

import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.addFields;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.addPrimaryKeys;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.addUpdateFields;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.addVersionComparisonClause;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.addVersionFields;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.getPrimaryKeys;

/**
 * This class is responsible for generating SQL UPSERT (INSERT ... ON CONFLICT) statements.
 *
 * <p>The generated SQL statement will insert or update rows in the specified table based on the provided event data.</p>
 *
 * <h3>Example:</h3>
 * Suppose we have a table named "tableName" with primary keys "key1" and "key2", and other data fields "data1" and "data2".
 * CdcEvent specific version fields will be used for versioning on the PostgreSQL side.
 * <pre>
 * INSERT INTO employees (key1, key2, data1, data2, topology_version, order, node_order)
 * VALUES (key1Val, key2Val, data1Val, data2Val, topologyVersionVal, orderVal, nodeOrderVal)
 * ON CONFLICT (key1, key2) DO UPDATE SET data1 = EXCLUDED.data1, data2 = EXCLUDED.data2,
 * topology_version = EXCLUDED.topology_version, order = EXCLUDED.order, node_order = EXCLUDED.node_order
 * WHERE (topology_version, order, node_order) <= (EXCLUDED.topology_version, EXCLUDED.order, EXCLUDED.node_order)
 * </pre>
 *
 * @see AbstractSqlWriter
 * @see CdcEvent
 */
public class UpsertSqlWriterImpl extends AbstractSqlWriter {
    /** */
    private final Set<String> fields;

    /** */
    private final Set<String> primaryFields;

    /**
     * @param entity     The {@link QueryEntity} representing the table and its metadata.
     * @param sqlValSize The maximum size of the SQL value string.
     */
    public UpsertSqlWriterImpl(QueryEntity entity, long sqlValSize) {
        super(entity, sqlValSize);

        this.fields = entity.getFields().keySet();
        this.primaryFields = getPrimaryKeys(entity);
    }

    /** {@inheritDoc} */
    @Override protected String prepareSqlTemplate(QueryEntity entity) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(entity.getTableName()).append(" (");

        addFields(entity, sql);

        sql.append(", ");

        addVersionFields(sql);

        sql.append(") VALUES %s ON CONFLICT (");

        addPrimaryKeys(entity, sql);

        sql.append(") DO UPDATE SET ");

        addUpdateFields(entity, sql);

        sql.append(" WHERE ");

        addVersionComparisonClause(sql);

        return sql.toString();
    }

    /** {@inheritDoc} */
    @Override protected String prepareDelimiter() {
        return ", ";
    }

    /** {@inheritDoc} */
    @Override public void addEvent(CdcEvent evt, StringBuilder valSql) {
        valSql.append('(');

        Iterator<String> itFields = fields.iterator();
        String field;

        while (itFields.hasNext()) {
            field = itFields.next();

            if (primaryFields.contains(field))
                valSql.append(((BinaryObject)evt.key()).field(field).toString());
            else
                valSql.append(((BinaryObject)evt.value()).field(field).toString());

            if (itFields.hasNext())
                valSql.append(", ");
        }

        // The same order as VERSION_FIELDS
        valSql.append(", ").append(evt.version().topologyVersion());
        valSql.append(", ").append(evt.version().order());
        valSql.append(", ").append(evt.version().nodeOrder());

        valSql.append(')');
    }
}
