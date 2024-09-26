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

package org.apache.ignite.cdc;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cdc.metrics.AbstractCdcMetrics;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UNDEFINED_CACHE_ID;

/**
 * Contains logic to process {@link CdcEvent} and apply them to the cluster.
 */
public abstract class AbstractCdcEventsApplier<K, V> {
    /** Maximum batch size. */
    private final int maxBatchSize;

    /** Update batch. */
    private final Map<K, V> updBatch = new HashMap<>();

    /** Remove batch. */
    private final Map<K, GridCacheVersion> rmvBatch = new HashMap<>();

    /** */
    private final BooleanSupplier hasUpdates = () -> !F.isEmpty(updBatch);

    /** */
    private final BooleanSupplier hasRemoves = () -> !F.isEmpty(rmvBatch);

    /** */
    private final IgniteLogger log;

    /** */
    private final AbstractCdcMetrics cdcMetrics;

    /**
     * @param maxBatchSize Maximum batch size.
     * @param log Logger.
     * @param cdcMetrics CDC client metrics.
     */
    public AbstractCdcEventsApplier(int maxBatchSize, IgniteLogger log, AbstractCdcMetrics cdcMetrics) {
        this.maxBatchSize = maxBatchSize;
        this.log = log.getLogger(getClass());
        this.cdcMetrics = cdcMetrics;
    }

    /**
     * @param evts Events to process.
     * @throws IgniteCheckedException If failed.
     */
    public void apply(Iterable<CdcEvent> evts) throws IgniteCheckedException {
        int currCacheId = UNDEFINED_CACHE_ID;
        int evtsApplied = 0;

        for (CdcEvent evt : evts) {
            if (log.isDebugEnabled())
                log.debug("Event received [evt=" + evt + ']');

            int cacheId = evt.cacheId();

            if (cacheId != currCacheId) {
                evtsApplied += applyIf(currCacheId, hasUpdates, hasRemoves);

                currCacheId = cacheId;
            }

            CacheEntryVersion order = evt.version();
            K key = toKey(evt);
            GridCacheVersion ver = new GridCacheVersion(order.topologyVersion(), order.order(), order.nodeOrder(), order.clusterId());

            if (evt.value() != null) {
                evtsApplied += applyIf(currCacheId, () -> isApplyBatch(updBatch, key), hasRemoves);

                updBatch.put(key, toValue(currCacheId, evt, ver));
            }
            else {
                evtsApplied += applyIf(currCacheId, hasUpdates, () -> isApplyBatch(rmvBatch, key));

                rmvBatch.put(key, ver);
            }
        }

        if (currCacheId != UNDEFINED_CACHE_ID)
            evtsApplied += applyIf(currCacheId, hasUpdates, hasRemoves);

        if (evtsApplied > 0) {
            cdcMetrics.addEventsSentCount(evtsApplied);
            cdcMetrics.setLastEventSentTime();

            if (log.isInfoEnabled())
                log.info("Events applied [evtsApplied=" + cdcMetrics.getEventsSentCount() + ']');
        }
    }

    /**
     * Applies data from {@link #updBatch} or {@link #rmvBatch} to Ignite if required.
     *
     * @param cacheId Current cache ID.
     * @param applyUpd Apply update batch flag supplier.
     * @param applyRmv Apply remove batch flag supplier.
     * @return Number of applied events.
     * @throws IgniteCheckedException In case of error.
     */
    private int applyIf(
        int cacheId,
        BooleanSupplier applyUpd,
        BooleanSupplier applyRmv
    ) throws IgniteCheckedException {
        int evtsApplied = 0;

        if (applyUpd.getAsBoolean()) {
            if (log.isDebugEnabled())
                log.debug("Applying put batch [cacheId=" + cacheId + ']');

            long start = System.nanoTime();

            putAllConflict(cacheId, updBatch);

            cdcMetrics.addPutAllTimeNanos(System.nanoTime() - start);

            evtsApplied += updBatch.size();

            updBatch.clear();
        }

        if (applyRmv.getAsBoolean()) {
            if (log.isDebugEnabled())
                log.debug("Applying remove batch [cacheId=" + cacheId + ']');

            long start = System.nanoTime();

            removeAllConflict(cacheId, rmvBatch);

            cdcMetrics.addRemoveAllTimeNanos(System.nanoTime() - start);

            evtsApplied += rmvBatch.size();

            rmvBatch.clear();
        }

        return evtsApplied;
    }

    /** @return {@code True} if update batch should be applied. */
    private boolean isApplyBatch(Map<K, ?> map, K key) {
        return map.size() >= maxBatchSize || map.containsKey(key);
    }

    /** @return Key. */
    protected abstract K toKey(CdcEvent evt);

    /** @return Value. */
    protected abstract V toValue(int cacheId, CdcEvent evt, GridCacheVersion ver);

    /** Stores DR data. */
    protected abstract void putAllConflict(int cacheId, Map<K, V> drMap);

    /** Removes DR data. */
    protected abstract void removeAllConflict(int cacheId, Map<K, GridCacheVersion> drMap);

    /** @return CDC client metrics. */
    public AbstractCdcMetrics metrics() {
        return cdcMetrics;
    }
}
