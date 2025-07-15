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

package org.apache.ignite.cdc.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.cdc.postgresql.IgniteToPostgreSqlCdcConsumer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class CdcPostgreSqlReplicationAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static final int MAX_BATCH_SIZE = 128;

    /** */
    protected static final int KEYS_CNT = 1024;

    /** */
    protected static void executeOnIgnite(IgniteEx src, String sqlText, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sqlText).setArgs(args);

        try (FieldsQueryCursor<List<?>> cursor = src.context().query().querySqlFields(qry, true)) {
            cursor.getAll();
        }
    }

    /** */
    protected static ResultSet selectOnPostgreSql(EmbeddedPostgres postgres, String qry) {
        try (Connection conn = postgres.getPostgresDatabase().getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(qry);

            return stmt.executeQuery();
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    protected static void executeOnPostgreSql(EmbeddedPostgres postgres, String qry) {
        try (Connection conn = postgres.getPostgresDatabase().getConnection()) {
            PreparedStatement stmt = conn.prepareStatement(qry);

            stmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    protected static boolean checkRow(
        EmbeddedPostgres postgres,
        String tableName,
        String columnName,
        String expected,
        String condition
    ) {
        String qry = "SELECT " + columnName + " FROM " + tableName + " WHERE " + condition;

        try (ResultSet res = selectOnPostgreSql(postgres, qry)) {
            if (res.next()) {
                String actual = res.getString(columnName);

                return expected.equals(actual);
            }

            return false;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** */
    protected static GridAbsPredicate waitForTablesCreatedOnPostgres(EmbeddedPostgres postgres, Set<String> caches) {
        return () -> {
            String sql = "SELECT EXISTS (" +
                "  SELECT 1 FROM information_schema.tables " +
                "  WHERE table_name = '%s'" +
                ")";

            for (String cache : caches) {
                try (ResultSet rs = selectOnPostgreSql(postgres, String.format(sql, cache.toLowerCase()))) {
                    rs.next();

                    if (!rs.getBoolean(1))
                        return false;
                }
                catch (SQLException e) {
                    log.error(e.getMessage(), e);

                    throw new IgniteException(e);
                }
            }

            return true;
        };
    }

    /** */
    protected static GridAbsPredicate waitForTableSize(EmbeddedPostgres postgres, String tableName, long expSz) {
        return () -> {
            try (ResultSet res = selectOnPostgreSql(postgres, "SELECT COUNT(*) FROM " + tableName)) {
                res.next();

                long cnt = res.getLong(1);

                return cnt == expSz;
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        };
    }

    /** */
    protected IgniteToPostgreSqlCdcConsumer getCdcConsumerConfiguration() {
        return new IgniteToPostgreSqlCdcConsumer()
            .setMaxBatchSize(MAX_BATCH_SIZE)
            .setOnlyPrimary(true)
            .setCreateTables(false);
    }

    /**
     * @param igniteCfg Ignite configuration.
     * @param caches Cache name set to stream to PostgreSql.
     * @param dataSrc Data Source.
     * @return Future for Change Data Capture application.
     */
    protected IgniteInternalFuture<?> startIgniteToPostgreSqlCdcConsumer(
        IgniteConfiguration igniteCfg,
        Set<String> caches,
        DataSource dataSrc
    ) {
        IgniteToPostgreSqlCdcConsumer cdcCnsmr = getCdcConsumerConfiguration()
            .setCaches(caches)
            .setDataSource(dataSrc);

        CdcConfiguration cdcCfg = new CdcConfiguration();

        cdcCfg.setConsumer(cdcCnsmr);

        return runAsync(new CdcMain(igniteCfg, null, cdcCfg), "ignite-src-to-postgres-" + igniteCfg.getConsistentId());
    }
}
