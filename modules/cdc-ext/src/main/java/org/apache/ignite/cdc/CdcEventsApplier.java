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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 * Contains logic to process {@link CdcEvent} and apply them to the provided by {@link #ignite()} cluster.
 */
public abstract class CdcEventsApplier {
    /** Maximum batch size. */
    private final int maxBatchSize;

    /** Caches. */
    private final Map<Integer, IgniteInternalCache<BinaryObject, BinaryObject>> ignCaches = new HashMap<>();

    /** Update batch. */
    private final Map<KeyCacheObject, GridCacheDrInfo> updBatch = new HashMap<>();

    /** Remove batch. */
    private final Map<KeyCacheObject, GridCacheVersion> rmvBatch = new HashMap<>();

    /** */
    private final BooleanSupplier hasUpdates = () -> !F.isEmpty(updBatch);

    /** */
    private final BooleanSupplier hasRemoves = () -> !F.isEmpty(rmvBatch);

    /** */
    protected final AtomicLong evtsApplied = new AtomicLong();

    /**
     * @param maxBatchSize Maximum batch size.
     */
    public CdcEventsApplier(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * @param evts Events to process.
     * @throws IgniteCheckedException If failed.
     */
    protected void apply(Iterable<CdcEvent> evts) throws IgniteCheckedException {
        IgniteInternalCache<BinaryObject, BinaryObject> currCache = null;

        for (CdcEvent evt : evts) {
            if (log().isDebugEnabled())
                log().debug("Event received [key=" + evt.key() + ']');

            IgniteInternalCache<BinaryObject, BinaryObject> cache = ignCaches.computeIfAbsent(evt.cacheId(), cacheId -> {
                for (String cacheName : ignite().cacheNames()) {
                    if (CU.cacheId(cacheName) == cacheId) {
                        // IgniteEx#cachex(String) will return null if cache not initialized with regular Ignite#cache(String) call.
                        ignite().cache(cacheName);

                        return ignite().cachex(cacheName);
                    }
                }

                throw new IllegalStateException("Cache with id not found [cacheId=" + cacheId + ']');
            });

            if (cache != currCache) {
                applyIf(currCache, hasUpdates, hasRemoves);

                currCache = cache;
            }

            CacheEntryVersion order = evt.version();

            KeyCacheObject key = new KeyCacheObjectImpl(evt.key(), null, evt.partition());

            if (evt.value() != null) {
                applyIf(currCache, () -> isApplyBatch(updBatch, key), hasRemoves);

                CacheObject val = new CacheObjectImpl(evt.value(), null);

                updBatch.put(key, new GridCacheDrInfo(val,
                    new GridCacheVersion(order.topologyVersion(), order.order(), order.nodeOrder(), order.clusterId())));
            }
            else {
                applyIf(currCache, hasUpdates, () -> isApplyBatch(rmvBatch, key));

                rmvBatch.put(key,
                    new GridCacheVersion(order.topologyVersion(), order.order(), order.nodeOrder(), order.clusterId()));
            }

            evtsApplied.incrementAndGet();
        }

        if (currCache != null)
            applyIf(currCache, hasUpdates, hasRemoves);
    }

    /**
     * Applies data from {@link #updBatch} or {@link #rmvBatch} to Ignite if required.
     *
     * @param cache Current cache.
     * @param applyUpd Apply update batch flag supplier.
     * @param applyRmv Apply remove batch flag supplier.
     * @throws IgniteCheckedException In case of error.
     */
    private void applyIf(
        IgniteInternalCache<BinaryObject, BinaryObject> cache,
        BooleanSupplier applyUpd,
        BooleanSupplier applyRmv
    ) throws IgniteCheckedException {
        if (applyUpd.getAsBoolean()) {
            if (log().isDebugEnabled())
                log().debug("Applying put batch [cache=" + cache.name() + ']');

            cache.putAllConflict(updBatch);

            updBatch.clear();
        }

        if (applyRmv.getAsBoolean()) {
            if (log().isDebugEnabled())
                log().debug("Applying remove batch [cache=" + cache.name() + ']');

            cache.removeAllConflict(rmvBatch);

            rmvBatch.clear();
        }
    }

    /** @return {@code True} if update batch should be applied. */
    private boolean isApplyBatch(Map<KeyCacheObject, ?> map, KeyCacheObject key) {
        return map.size() >= maxBatchSize || map.containsKey(key);
    }

    /** @return Ignite instance. */
    protected abstract IgniteEx ignite();

    /** @return Logger. */
    protected abstract IgniteLogger log();
}
