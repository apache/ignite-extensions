package org.apache.ignite.cdc.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cdc.CdcEvent;

/**
 * Manages replication of CDC (Change Data Capture) events from Apache Ignite to PostgreSQL.
 *
 * <p>This manager registers handlers for different cache IDs and delegates the application of specific CDC events
 * to these handlers.</p>
 *
 * <p>When an event arrives, the handler associated with the event’s cache ID processes the event by producing
 * SQL queries and accumulating them into a batch for efficient bulk insertion.</p>
 */
public class IgniteToPostgreSqlCdcManager {
    /** */
    private final Map<Integer, IgniteToPostgreSqlCdcApplier> appliers = new ConcurrentHashMap<>();

    /**
     * @param cacheId Unique identifier of the cache.
     * @param applier Applicator instance capable of handling CDC events for this cache.
     */
    public void registerHandler(int cacheId, IgniteToPostgreSqlCdcApplier applier) {
        appliers.put(cacheId, applier);
    }

    /**
     * @param evt CdcEvent
     * @param conn Opened database connection for issuing SQL queries.
     * @param batch Batch list where prepared SQL statements are stored.
     * @return Number of events handled (typically either 0 or 1 or 2).
     * @throws IgniteException Wrapped exception if no applicable handler exists or an issue arises during event processing.
     */
    public byte handleEvent(CdcEvent evt, Connection conn, List<PreparedStatement> batch) throws IgniteException {
        IgniteToPostgreSqlCdcApplier applier = appliers.get(evt.cacheId());

        if (applier == null)
            throw new IllegalStateException("No applier registered for cacheId: " + evt.cacheId());

        try {
            return applier.handleEvent(evt, conn, batch);
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
    }
}
