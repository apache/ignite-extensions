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

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.metrics.IgniteToIgniteCdcMetrics;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer.DFLT_IS_ONLY_PRIMARY;

/**
 * Change Data Consumer that streams all data changes to destination cluster by the provided {@link #applier}.
 *
 * @see AbstractCdcEventsApplier
 */
public abstract class AbstractIgniteCdcStreamer implements CdcConsumer {
    /** Handle only primary entry flag. */
    private boolean onlyPrimary = DFLT_IS_ONLY_PRIMARY;

    /** Cache names. */
    private Set<String> caches;

    /** Cache IDs. */
    protected Set<Integer> cachesIds;

    /** Maximum batch size. */
    protected int maxBatchSize;

    /** Events applier. */
    protected AbstractCdcEventsApplier<?, ?> applier;

    /** CDC metrics. */
    protected IgniteToIgniteCdcMetrics cdcMetrics;

    /** Logger. */
    @LoggerResource
    protected IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry reg) {
        A.notEmpty(caches, "caches");

        cachesIds = caches.stream()
            .mapToInt(CU::cacheId)
            .boxed()
            .collect(Collectors.toSet());

        cdcMetrics = new IgniteToIgniteCdcMetrics((MetricRegistryImpl)reg);
    }

    /** {@inheritDoc} */
    @Override public boolean onEvents(Iterator<CdcEvent> events) {
        try {
            applier.apply(() -> F.iterator(
                events,
                F.identity(),
                true,
                evt -> !onlyPrimary || evt.primary(),
                evt -> F.isEmpty(cachesIds) || cachesIds.contains(evt.cacheId()),
                evt -> evt.version().otherClusterVersion() == null));

            return true;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
        cacheEvents.forEachRemaining(e -> {
            // Just skip. Handle of cache events not supported.
        });
    }

    /** {@inheritDoc} */
    @Override public void onCacheDestroy(Iterator<Integer> caches) {
        caches.forEachRemaining(e -> {
            // Just skip. Handle of cache events not supported.
        });
    }

    /** {@inheritDoc} */
    @Override public void onMappings(Iterator<TypeMapping> mappings) {
        mappings.forEachRemaining(mapping -> {
            registerMapping(binaryContext(), log, mapping);

            cdcMetrics.incrementMappingsSentCount();
        });

        cdcMetrics.setLastEventSentTime();
    }

    /** {@inheritDoc} */
    @Override public void onTypes(Iterator<BinaryType> types) {
        types.forEachRemaining(t -> {
            BinaryMetadata meta = ((BinaryTypeImpl)t).metadata();

            registerBinaryMeta(binaryContext(), log, meta);

            cdcMetrics.incrementTypesSentCount();
        });

        cdcMetrics.setLastEventSentTime();
    }

    /**
     * Register {@code meta}.
     *
     * @param ctx Binary context.
     * @param log Logger.
     * @param meta Binary metadata to register.
     */
    public static void registerBinaryMeta(BinaryContext ctx, IgniteLogger log, BinaryMetadata meta) {
        ctx.updateMetadata(meta.typeId(), meta, false);

        if (log.isInfoEnabled())
            log.info("BinaryMeta [meta=" + meta + ']');
    }

    /**
     * Register {@code mapping}.
     *
     * @param ctx Binary context.
     * @param log Logger.
     * @param mapping Type mapping to register.
     */
    public static void registerMapping(BinaryContext ctx, IgniteLogger log, TypeMapping mapping) {
        assert mapping.platformType().ordinal() <= Byte.MAX_VALUE;

        byte platformType = (byte)mapping.platformType().ordinal();

        ctx.registerUserClassName(mapping.typeId(), mapping.typeName(), false, false, platformType);

        if (log.isInfoEnabled())
            log.info("Mapping [mapping=" + mapping + ']');
    }

    /** @return Binary context. */
    protected abstract BinaryContext binaryContext();

    /**
     * Sets whether entries only from primary nodes should be handled.
     *
     * @param onlyPrimary Whether entries only from primary nodes should be handled.
     * @return {@code this} for chaining.
     */
    public AbstractIgniteCdcStreamer setOnlyPrimary(boolean onlyPrimary) {
        this.onlyPrimary = onlyPrimary;

        return this;
    }

    /**
     * Sets cache names that participate in CDC.
     *
     * @param caches Cache names.
     * @return {@code this} for chaining.
     */
    public AbstractIgniteCdcStreamer setCaches(Set<String> caches) {
        this.caches = caches;

        return this;
    }

    /**
     * Sets maximum batch size that will be applied to destination cluster.
     *
     * @param maxBatchSize Maximum batch size.
     * @return {@code this} for chaining.
     */
    public AbstractIgniteCdcStreamer setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;

        return this;
    }
}
