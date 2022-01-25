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

package org.apache.ignite.cdc.conflictresolve;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Intermediate component to provide {@link CacheVersionConflictResolverImpl} for specific cache.
 *
 * @see CacheVersionConflictResolverImpl
 * @see CacheVersionConflictResolver
 */
public class CacheConflictResolutionManagerImpl<K, V> implements CacheConflictResolutionManager<K, V> {
    /**
     * Field for conflict resolve.
     * Value of this field will be used to compare two entries in case of conflicting changes.
     * Note, values of this field must implement {@link Comparable}.
     *
     * @see CacheVersionConflictResolverImpl
     */
    private final String conflictResolveField;

    /** CLuster Id. */
    private final byte clusterId;

    /** Grid cache context. */
    private GridCacheContext<K, V> cctx;

    /**
     * @param conflictResolveField Field to resolve conflicts.
     */
    public CacheConflictResolutionManagerImpl(String conflictResolveField, byte clusterId) {
        this.conflictResolveField = conflictResolveField;
        this.clusterId = clusterId;
    }

    /** {@inheritDoc} */
    @Override public CacheVersionConflictResolver conflictResolver() {
        cctx.logger(this.getClass()).info("Conflict resolver created [" +
            "cache=" + cctx.name() +
            ", clusterId=" +  clusterId +
            ", conflictResolveField=" + conflictResolveField + ']'
        );

        IgniteLogger log = cctx.logger(CacheVersionConflictResolverImpl.class);

        if (log.isDebugEnabled()) {
            return new DebugCacheVersionConflictResolverImpl(
                clusterId,
                conflictResolveField,
                log
            );
        }

        return new CacheVersionConflictResolverImpl(
            clusterId,
            conflictResolveField,
            log
        );
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheContext<K, V> cctx) {
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel, boolean destroy) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        // No-op.
    }
}
