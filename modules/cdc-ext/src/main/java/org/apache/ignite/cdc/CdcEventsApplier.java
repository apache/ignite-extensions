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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration;
import org.apache.ignite.cdc.kafka.KafkaToIgniteMetadataUpdater;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 * Contains logic to process {@link CdcEvent} and apply them to the provided by {@link #ignite()} cluster.
 */
public abstract class CdcEventsApplier {
    /** Maximum batch size. */
    protected int maxBatchSize;

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

    /**
     * Update closure.
     * @see #applyWithRetry(IgniteInClosureX, IgniteInternalCache)
     */
    private final IgniteInClosureX<IgniteInternalCache<BinaryObject, BinaryObject>> updClo =
        new IgniteInClosureX<IgniteInternalCache<BinaryObject, BinaryObject>>() {
            /** {@inheritDoc} */
            @Override public void applyx(
                IgniteInternalCache<BinaryObject, BinaryObject> cache
            ) throws IgniteCheckedException {
                cache.putAllConflict(updBatch);
            }
        };

    /**
     * Remove closure.
     * @see #applyWithRetry(IgniteInClosureX, IgniteInternalCache)
     */
    private final IgniteInClosureX<IgniteInternalCache<BinaryObject, BinaryObject>> rmvClo =
        new IgniteInClosureX<IgniteInternalCache<BinaryObject, BinaryObject>>() {
            /** {@inheritDoc} */
            @Override public void applyx(
                IgniteInternalCache<BinaryObject, BinaryObject> cache
            ) throws IgniteCheckedException {
                cache.removeAllConflict(rmvBatch);
            }
        };

    /**
     * @param maxBatchSize Maximum batch size.
     */
    public CdcEventsApplier(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * @param evts Events to process.
     * @return Number of applied events.
     * @throws IgniteCheckedException If failed.
     */
    protected int apply(Iterable<CdcEvent> evts) throws IgniteCheckedException {
        IgniteInternalCache<BinaryObject, BinaryObject> currCache = null;

        int evtsApplied = 0;

        for (CdcEvent evt : evts) {
            if (log().isDebugEnabled())
                log().debug("Event received [evt=" + evt + ']');

            IgniteInternalCache<BinaryObject, BinaryObject> cache = ignCaches.computeIfAbsent(evt.cacheId(), cacheId -> {
                for (String cacheName : ignite().cacheNames()) {
                    if (CU.cacheId(cacheName) == cacheId) {
                        // IgniteEx#cachex(String) will return null if cache not initialized with regular Ignite#cache(String) call.
                        ignite().cache(cacheName);

                        IgniteInternalCache<Object, Object> cache0 = ignite().cachex(cacheName);

                        assert cache0 != null;

                        return cache0.keepBinary();
                    }
                }

                throw new IllegalStateException("Cache with id not found [cacheId=" + cacheId + ']');
            });

            if (cache != currCache) {
                evtsApplied += applyIf(currCache, hasUpdates, hasRemoves);

                currCache = cache;
            }

            CacheEntryVersion order = evt.version();

            KeyCacheObject key;

            if (evt.key() instanceof KeyCacheObject)
                key = (KeyCacheObject)evt.key();
            else
                key = new KeyCacheObjectImpl(evt.key(), null, evt.partition());

            if (evt.value() != null) {
                evtsApplied += applyIf(currCache, () -> isApplyBatch(updBatch, key), hasRemoves);

                CacheObject val;

                if (evt.value() instanceof CacheObject)
                    val = (CacheObject)evt.value();
                else
                    val = new CacheObjectImpl(evt.value(), null);

                updBatch.put(key, new GridCacheDrInfo(val,
                    new GridCacheVersion(order.topologyVersion(), order.order(), order.nodeOrder(), order.clusterId())));
            }
            else {
                evtsApplied += applyIf(currCache, hasUpdates, () -> isApplyBatch(rmvBatch, key));

                rmvBatch.put(key,
                    new GridCacheVersion(order.topologyVersion(), order.order(), order.nodeOrder(), order.clusterId()));
            }
        }

        if (currCache != null)
            evtsApplied += applyIf(currCache, hasUpdates, hasRemoves);

        return evtsApplied;
    }

    /**
     * Applies data from {@link #updBatch} or {@link #rmvBatch} to Ignite if required.
     *
     * @param cache Current cache.
     * @param applyUpd Apply update batch flag supplier.
     * @param applyRmv Apply remove batch flag supplier.
     * @return Number of applied events.
     */
    private int applyIf(
        IgniteInternalCache<BinaryObject, BinaryObject> cache,
        BooleanSupplier applyUpd,
        BooleanSupplier applyRmv
    ) {
        int evtsApplied = 0;

        if (applyUpd.getAsBoolean()) {
            if (log().isDebugEnabled())
                log().debug("Applying put batch [cache=" + cache.name() + ']');

            applyWithRetry(updClo, cache);

            evtsApplied += updBatch.size();

            updBatch.clear();
        }

        if (applyRmv.getAsBoolean()) {
            if (log().isDebugEnabled())
                log().debug("Applying remove batch [cache=" + cache.name() + ']');

            applyWithRetry(rmvClo, cache);

            evtsApplied += rmvBatch.size();

            rmvBatch.clear();
        }

        return evtsApplied;
    }

    /**
     * Executes closure with retry logic.
     * Metadata update thread polls metadata asynchronously with {@link KafkaToIgniteCdcStreamerConfiguration#getMetaUpdateInterval()}
     * interval. This means metadata updates can be seen later than data updates. In this case {@link BinaryObjectException} can point
     * to absence of metadata. To overcome lack of metadata invoke {@link #updateMetadata()} and retry closure.
     *
     * @param clo Closure to apply.
     * @param cache Cache for closure.
     * @see KafkaToIgniteMetadataUpdater
     * @see KafkaToIgniteCdcStreamerConfiguration#getMetaUpdateInterval()
     */
    private void applyWithRetry(
        IgniteInClosureX<IgniteInternalCache<BinaryObject, BinaryObject>> clo,
        IgniteInternalCache<BinaryObject, BinaryObject> cache
    ) {
        try {
            clo.apply(cache);
        }
        catch (Exception e) {
            // Retry only if cause is BinaryObjectException.
            if (!X.hasCause(e, BinaryObjectException.class))
                throw e;

            // Retry after metadata update.
            updateMetadata();

            clo.apply(cache);
        }
    }

    /** @return {@code True} if update batch should be applied. */
    private boolean isApplyBatch(Map<KeyCacheObject, ?> map, KeyCacheObject key) {
        return map.size() >= maxBatchSize || map.containsKey(key);
    }

    /**
     * Register {@code meta} inside {@code ign} instance.
     *
     * @param ign Ignite instance.
     * @param log Logger.
     * @param meta Binary metadata to register.
     */
    public static void registerBinaryMeta(IgniteEx ign, IgniteLogger log, BinaryMetadata meta) {
        ign.context().cacheObjects().addMeta(
            meta.typeId(),
            new BinaryTypeImpl(
                ((CacheObjectBinaryProcessorImpl)ign.context().cacheObjects()).binaryContext(),
                meta
            ),
            false
        );

        if (log.isInfoEnabled())
            log.info("BinaryMeta[meta=" + meta + ']');
    }

    /**
     * Register {@code mapping} inside {@code ign} instance.
     *
     * @param ign Ignite instance.
     * @param log Logger.
     * @param mapping Type mapping to register.
     */
    public static void registerMapping(IgniteEx ign, IgniteLogger log, TypeMapping mapping) {
        assert mapping.platformType().ordinal() <= Byte.MAX_VALUE;

        try {
            ign.context().marshallerContext().registerClassName(
                (byte)mapping.platformType().ordinal(),
                mapping.typeId(),
                mapping.typeName(),
                false
            );

            if (log.isInfoEnabled())
                log.info("Mapping[mapping=" + mapping + ']');
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** Update metadata if possible. */
    protected void updateMetadata() {
        // No-op.
    }

    /** @return Ignite instance. */
    protected abstract IgniteEx ignite();

    /** @return Logger. */
    protected abstract IgniteLogger log();
}
