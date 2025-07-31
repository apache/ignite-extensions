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

package org.apache.ignite.cdc.postgresql;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
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
    public static final String EVTS_SENT_CNT = "EventsCount";

    /** */
    public static final String EVTS_SENT_CNT_DESC = "Count of events applied to PostgreSQL";

    /** */
    public static final String LAST_EVT_SENT_TIME = "LastEventTime";

    /** */
    public static final String LAST_EVT_SENT_TIME_DESC = "Timestamp of last applied event to PostgreSQL";

    /** */
    private static final boolean DFLT_IS_ONLY_PRIMARY = true;

    /** */
    private static final long DFLT_BATCH_SIZE = 1024;

    /** */
    private static final boolean DFLT_CREATE_TABLES = false;

    /** */
    private DataSource dataSrc;

    /** Collection of cache names which will be replicated to PostgreSQL. */
    private Collection<String> caches;

    /** */
    private boolean onlyPrimary = DFLT_IS_ONLY_PRIMARY;

    /** */
    private long batchSize = DFLT_BATCH_SIZE;

    /** */
    private boolean createTables = DFLT_CREATE_TABLES;

    /** Log. */
    @LoggerResource
    private IgniteLogger log;

    /** Cache IDs. */
    private Set<Integer> cachesIds;

    /** Applier instance responsible for applying individual CDC events to PostgreSQL. */
    private IgniteToPostgreSqlCdcApplier applier;

    /** Count of events applied to PostgreSQL. */
    private AtomicLongMetric evtsCnt;

    /** Timestamp of last applied batch to PostgreSQL. */
    private AtomicLongMetric lastEvtTs;

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry reg) {
        A.notNull(dataSrc, "dataSource");
        A.notEmpty(caches, "caches");
        A.ensure(batchSize > 0, "batchSize");

        cachesIds = caches.stream()
            .map(CU::cacheId)
            .collect(Collectors.toSet());

        applier = new IgniteToPostgreSqlCdcApplier(dataSrc, batchSize, log);

        MetricRegistryImpl mreg = (MetricRegistryImpl)reg;

        this.evtsCnt = mreg.longMetric(EVTS_SENT_CNT, EVTS_SENT_CNT_DESC);
        this.lastEvtTs = mreg.longMetric(LAST_EVT_SENT_TIME, LAST_EVT_SENT_TIME_DESC);

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

        long evtsSent = applier.applyEvents(filtered);

        if (evtsSent > 0) {
            evtsCnt.add(evtsSent);
            lastEvtTs.value(System.currentTimeMillis());

            if (log.isInfoEnabled())
                log.info("Events applied [evtsApplied=" + evtsCnt.value() + ']');
        }

        return true;
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

        long tablesCreated = applier.applyCacheEvents(filtered, createTables);

        if (tablesCreated > 0 && log.isInfoEnabled())
            log.info("Cache changes applied [tablesCreatedCnt=" + tablesCreated + ']');
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
     * Sets cache names to replicate.
     *
     * @param caches Cache names.
     * @return {@code this} for chaining.
     */
    public IgniteToPostgreSqlCdcConsumer setCaches(Set<String> caches) {
        this.caches = Collections.unmodifiableSet(caches);

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
     * Sets the batch size that will be submitted to PostgreSQL.
     * <p>
     * This setting controls how many statements are sent in a single {@link java.sql.PreparedStatement#executeBatch()} call.
     * <p>
     *
     * @param batchSize number of statements per batch.
     * @return {@code this} for chaining.
     */
    public IgniteToPostgreSqlCdcConsumer setBatchSize(int batchSize) {
        this.batchSize = batchSize;

        return this;
    }

    /**
     * Enables/disables creation of tables by this instance.
     *
     * @param createTables True to create tables on start-up
     * @return {@code this} for chaining.
     */
    public IgniteToPostgreSqlCdcConsumer setCreateTables(boolean createTables) {
        this.createTables = createTables;

        return this;
    }
}
