package org.apache.ignite.cdc.postgresql.operations;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.cache.QueryEntity;

import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.VERSION_FIELDS;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.addFieldsForDdl;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.addPrimaryKeys;
import static org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcUtils.addVersionFields;

/**
 * Class that implements the logic for creating a table and indexes for the Ignite-to-PostgreSQL connector.
 * The class sends a CREATE TABLE IF NOT EXISTS query to the database to create a table if it does not exist.
 * It also sends CREATE INDEX queries to create indexes on the version fields and key and version fields.
 */
public class CreateSqlWriter {
    /** */
    private static final String VERSION_IDX_NAME = "idx_version_composite";

    /** */
    private static final String KEY_VERSION_IDX_NAME = "idx_key_version_covering";

    /** */
    private final QueryEntity entity;

    /**
     * @param entity QueryEntity object.
     */
    public CreateSqlWriter(QueryEntity entity) {
        this.entity = entity;
    }

    /**
     * @param conn Connection object.
     * @throws SQLException if a database access error occurs.
     */
    public void createTableIfNotExists(Connection conn) throws SQLException {
        String ddl = getDdlSql();
        String verIdx = getVersionIdxSql();
        String keyVerIdx = getKeyVersionIdxSql();

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
            stmt.execute(verIdx);
            stmt.execute(keyVerIdx);
        }
    }

    /**
     * Generates the SQL statement for creating a table.
     *
     * @return SQL statement for creating a table.
     */
    private String getDdlSql() {
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(entity.getTableName()).append(" (");

        addFieldsForDdl(entity, ddl);

        // VERSION_FIELDS is an ordered map
        VERSION_FIELDS.forEach((verField, type) -> ddl.append(", ").append(verField).append(" ").append(type));

        ddl.append(", PRIMARY KEY (");

        addPrimaryKeys(entity, ddl);

        ddl.append(')').append(')');

        return ddl.toString();
    }

    /**
     * Generates the SQL statement for creating an index on the version fields.
     *
     * @return SQL statement for creating an index on the version fields.
     */
    private String getVersionIdxSql() {
        StringBuilder idxSqlBuilder = new StringBuilder("CREATE INDEX ")
            .append(VERSION_IDX_NAME).append(" ON ").append(entity.getTableName()).append(" (");

        addVersionFields(idxSqlBuilder);

        idxSqlBuilder.append(')');

        return idxSqlBuilder.toString();
    }

    /**
     * Generates the SQL statement for creating an index on the key and version fields.
     *
     * @return SQL statement for creating an index on the key and version fields.
     */
    private String getKeyVersionIdxSql() {
        StringBuilder idxSqlBuilder = new StringBuilder("CREATE INDEX ")
            .append(KEY_VERSION_IDX_NAME).append(" ON ").append(entity.getTableName()).append(" (");

        addPrimaryKeys(entity, idxSqlBuilder);

        idxSqlBuilder.append(", ");

        addVersionFields(idxSqlBuilder);

        idxSqlBuilder.append(')');

        return idxSqlBuilder.toString();
    }
}
