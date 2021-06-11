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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class implements simple conflict resolution algorithm.
 * Algorithm decides which version of the entry should be used "new" or "old".
 * The following steps performed:
 * <ul>
 *     <li>If entry is freshly created then new version used - {@link GridCacheVersionedEntryEx#isStartVersion()}.</li>
 *     <li>If change made in this cluster then new version used - {@link GridCacheVersionedEntryEx#dataCenterId()}.</li>
 *     <li>If cluster of new entry equal to cluster of old entry
 *     then entry with the greater {@link GridCacheVersionedEntryEx#order()} used.</li>
 *     <li>If {@link #conflictResolveField} provided and field of new entry greater then new version used.</li>
 *     <li>If {@link #conflictResolveField} provided and field of old entry greater then old version used.</li>
 *     <li>Entry with the lower value of {@link GridCacheVersionedEntryEx#dataCenterId()} used.</li>
 * </ul>
 *
 * Note, data center with lower value has greater priority e.g first (1) data center is main in case conflict can't be resolved
 * automatically.
 */
public class CacheVersionConflictResolverImpl implements CacheVersionConflictResolver {
    /**
     * Cluster id.
     * Note, cluster with lower value has greater priority e.g first (1) cluster is main in case conflict can't be resolved automatically.
     */
    private final byte clusterId;

    /**
     * Field for conflict resolve.
     * Value of this field will be used to compare two entries in case of conflicting changes.
     * values of this field must implement {@link Comparable} interface.
     * <pre><i>Note, value of this field used to resolve conflict for external updates only.</i>
     *
     * @see CacheVersionConflictResolverImpl
     */
    private final String conflictResolveField;

    /** Logger. */
    private final IgniteLogger log;

    /** If {@code true} then conflict resolving with the value field enabled. */
    private boolean conflictResolveFieldEnabled;

    /**
     * @param clusterId Data center id.
     * @param conflictResolveField Field to resolve conflicts.
     * @param log Logger.
     */
    public CacheVersionConflictResolverImpl(byte clusterId, String conflictResolveField, IgniteLogger log) {
        this.clusterId = clusterId;
        this.conflictResolveField = conflictResolveField;
        this.log = log;

        conflictResolveFieldEnabled = conflictResolveField != null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCacheVersionConflictContext<K, V> resolve(
        CacheObjectValueContext ctx,
        GridCacheVersionedEntryEx<K, V> oldEntry,
        GridCacheVersionedEntryEx<K, V> newEntry,
        boolean atomicVerComparator
    ) {
        GridCacheVersionConflictContext<K, V> res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

        if (isUseNew(ctx, oldEntry, newEntry))
            res.useNew();
        else {
            log.warning("Skip update due to the conflict [key=" + newEntry.key() + ", fromCluster=" + newEntry.dataCenterId()
                + ", toCluster=" + oldEntry.dataCenterId() + ']');

            res.useOld();
        }

        return res;
    }

    /**
     * @param ctx Context.
     * @param oldEntry Old entry.
     * @param newEntry New entry.
     * @param <K> Key type.
     * @param <V> Key type.
     * @return {@code True} is should use new entry.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <K, V> boolean isUseNew(
        CacheObjectValueContext ctx,
        GridCacheVersionedEntryEx<K, V> oldEntry,
        GridCacheVersionedEntryEx<K, V> newEntry
    ) {
        if (newEntry.dataCenterId() == clusterId) // Update made on the local cluster always win.
            return true;

        if (oldEntry.isStartVersion()) // New entry.
            return true;

        if (oldEntry.dataCenterId() == newEntry.dataCenterId())
            return newEntry.version().compareTo(oldEntry.version()) > 0; // New version from the same cluster.

        if (conflictResolveFieldEnabled) {
            Object oldVal = oldEntry.value(ctx);
            Object newVal = newEntry.value(ctx);

            if (oldVal != null && newVal != null) {
                Comparable o;
                Comparable n;

                try {
                    if (oldVal instanceof BinaryObject) {
                        o = ((BinaryObject)oldVal).field(conflictResolveField);
                        n = ((BinaryObject)newVal).field(conflictResolveField);
                    }
                    else {
                        o = U.field(oldVal, conflictResolveField);
                        n = U.field(newVal, conflictResolveField);
                    }

                    if (o == null || n == null)
                        return o == null;

                    return o.compareTo(n) < 0;
                }
                catch (Exception e) {
                    log.error("Error while resolving replication conflict with field '" +
                        conflictResolveField + "'.\nConflict resolve with the field disabled.", e);

                    conflictResolveFieldEnabled = false;
                }
            }
        }

        // Cluster with the lower ID have biggest priority(e.g. first cluster is main).
        return newEntry.dataCenterId() < oldEntry.dataCenterId();
    }
}
