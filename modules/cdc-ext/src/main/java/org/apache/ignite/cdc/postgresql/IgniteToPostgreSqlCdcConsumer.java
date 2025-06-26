package org.apache.ignite.cdc.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.resources.LoggerResource;

/**
 * This class represents a consumer component that replicates cache changes from Apache Ignite to PostgreSQL using
 * Change Data Capture (CDC) mechanism. It applies events to PostgreSQL via batch-prepared SQL statements, ensuring
 * efficient handling of large volumes of updates.
 *
 * <p>Additionally, it provides methods for initializing connections, managing transactions, and performing atomic batches
 * of writes.</p>
 */
public class IgniteToPostgreSqlCdcConsumer implements CdcConsumer {
    /** */
    private static final boolean DFLT_IS_ONLY_PRIMARY = false;

    /** */
    private static final long DFLT_SQL_BATCH_SIZE = 1024;

    /** */
    private static final long DFLT_SQL_VALUE_SIZE = 100;

    /** Manager instance responsible for applying individual CDC events to PostgreSQL. */
    private final IgniteToPostgreSqlCdcManager replicationMgr = new IgniteToPostgreSqlCdcManager();

    /** */
    private DataSource dataSrc;

    /** Collection of cache names which will be replicated to PostgreSQL. */
    private Collection<String> caches;

    /** */
    private boolean onlyPrimary = DFLT_IS_ONLY_PRIMARY;

    /** */
    private long sqlBatchSize = DFLT_SQL_BATCH_SIZE;

    /** */
    private long sqlValSize = DFLT_SQL_VALUE_SIZE;

    /** Log. */
    @LoggerResource
    private IgniteLogger log;

    /** Cache IDs. */
    private Set<Integer> cachesIds;

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry mreg) {
        A.notNull(dataSrc, "dataSource");
        A.notEmpty(caches, "cachesToReplicate");

        cachesIds = caches.stream()
            .map(CU::cacheId)
            .collect(Collectors.toSet());

        if (log.isInfoEnabled())
            log.info("CDC Ignite to PostgreSQL start-up [cacheIds=" + cachesIds + ']');
    }

    /** {@inheritDoc} */
    @Override public boolean onEvents(Iterator<CdcEvent> events) {
        Iterator<CdcEvent> filtered = F.iterator(
            events,
            F.identity(),
            true,
            evt -> !onlyPrimary || evt.primary(),
            evt -> cachesIds.contains(evt.cacheId()));

        return withTx((conn) -> {
            List<PreparedStatement> activeBatch = new ArrayList<>();

            int cnt = 0;

            while (filtered.hasNext()) {
                CdcEvent evt = filtered.next();

                cnt += replicationMgr.handleEvent(evt, conn, activeBatch);

                if (cnt >= sqlBatchSize) {
                    flush(activeBatch);
                    cnt = 0;
                }
            }

            flush(activeBatch);
        });
    }

    /**
     * Flushes accumulated SQL statements by executing each PreparedStatement's batch.
     *
     * @param batch List of prepared statements waiting to be executed.
     */
    private void flush(List<PreparedStatement> batch) {
        try {
            for (PreparedStatement ps : batch)
                ps.executeBatch();

            batch.clear();
        }
        catch (SQLException sqlException) {
            throw new IgniteException(sqlException);
        }
    }

    /** {@inheritDoc} */
    @Override public void onTypes(Iterator<BinaryType> types) {
        types.forEachRemaining(e -> {
            // Just skip. Handle of cache events not supported.
        });
    }

    /** {@inheritDoc} */
    @Override public void onMappings(Iterator<TypeMapping> mappings) {
        mappings.forEachRemaining(e -> {
            // Just skip. Handle of cache events not supported.
        });
    }

    /** {@inheritDoc} */
    @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
        Iterator<CdcCacheEvent> filtered = F.iterator(
            cacheEvents,
            F.identity(),
            true,
            evt -> cachesIds.contains(evt.cacheId()));

        withTx((conn) -> {
            while (filtered.hasNext()) {
                CdcCacheEvent cacheEvt = filtered.next();

                int cacheId = cacheEvt.cacheId();

                assert cacheEvt.queryEntities().size() == 1 : "There should be exactly 1 QueryEntity for cacheId: " + cacheId;

                QueryEntity entity = cacheEvt.queryEntities().iterator().next();

                IgniteToPostgreSqlCdcApplier hnd = new IgniteToPostgreSqlCdcApplier(entity, sqlValSize);

                try {
                    hnd.createTableIfNotExists(conn);
                }
                catch (SQLException e) {
                    throw new IgniteException(e);
                }

                replicationMgr.registerHandler(cacheId, hnd);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onCacheDestroy(Iterator<Integer> caches) {
        caches.forEachRemaining(e -> {
            // Just skip. Handle of cache events not supported.
        });
    }

    /** {@inheritDoc} */
    @Override public void stop() {

    }

    /**
     * Executes the given operation inside a database transaction, providing a Connection instance.
     *
     * @param op Function accepting a Connection argument.
     * @return True upon successful completion of the operation.
     */
    private boolean withTx(IgniteThrowableConsumer<Connection> op) {
        Connection conn = null;

        try {
            conn = dataSrc.getConnection();
            conn.setAutoCommit(false);

            op.accept(conn);

            conn.commit();

            return true;
        }
        catch (Throwable e) {
            try {
                if (conn != null && !conn.isClosed())
                    conn.rollback();
            }
            catch (SQLException rollbackEx) {
                e.addSuppressed(rollbackEx);
            }

            throw new IgniteException("CDC failure, transaction rolled back", e);
        }
        finally {
            try {
                if (conn != null && !conn.isClosed())
                    conn.close();
            }
            catch (SQLException closeEx) {
                log.warning("Error closing connection", closeEx);
            }
        }
    }

    /**
     * Sets the datasource configuration for connecting to the PostgreSQL database.
     *
     * @param dataSrc Configured data source.
     * @return {@code this} for chaining.
     */
    public IgniteToPostgreSqlCdcConsumer setDataSource(DataSource dataSrc) {
        this.dataSrc = dataSrc;

        return this;
    }

    /**
     * Specifies the collection of cache names whose changes need to be replicated to PostgreSQL.
     *
     * @param caches Collection of cache names.
     * @return {@code this} for chaining.
     */
    public IgniteToPostgreSqlCdcConsumer setCachesToReplicate(Collection<String> caches) {
        this.caches = caches;

        return this;
    }

    /**
     * Enables/disables filtering to accept only primary-node originated events.
     *
     * @param onlyPrimary True to restrict replication to primary events only.
     * @return {@code this} for chaining.
     */
    public IgniteToPostgreSqlCdcConsumer setOnlyPrimary(boolean onlyPrimary) {
        this.onlyPrimary = onlyPrimary;

        return this;
    }

    /**
     * Adjusts the number of events after which the batch is flushed to the database.
     *
     * @param sqlBatchSize New batch size setting.
     * @return {@code this} for chaining.
     */
    public IgniteToPostgreSqlCdcConsumer setSqlBatchSize(long sqlBatchSize) {
        this.sqlBatchSize = sqlBatchSize;

        return this;
    }

    /**
     * Changes the threshold for splitting large values across multiple SQL rows.
     *
     * @param sqlValSize Threshold value size (in bytes).
     * @return {@code this} for chaining.
     */
    public IgniteToPostgreSqlCdcConsumer setSqlValueSize(long sqlValSize) {
        this.sqlValSize = sqlValSize;

        return this;
    }
}
