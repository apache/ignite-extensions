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
 *     <li>Conflict can't be resolved. Update ignored. Old version used.</li>
 * </ul>
 */
public class CacheVersionConflictResolverImpl implements CacheVersionConflictResolver {
    /**
     * Cluster id.
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
    private final boolean conflictResolveFieldEnabled;

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
        else
            res.useOld();

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

        if (oldEntry.isStartVersion()) // Entry miss (new entry).
            return true;

        if (oldEntry.dataCenterId() == newEntry.dataCenterId())
            return newEntry.version().compareTo(oldEntry.version()) > 0; // New version from the same cluster.

        if (conflictResolveFieldEnabled) {
            Object oldVal = oldEntry.value(ctx);
            Object newVal = newEntry.value(ctx);

            if (oldVal != null && newVal != null) {
                Comparable oldResolveField;
                Comparable newResolveField;

                try {
                    if (oldVal instanceof BinaryObject) {
                        oldResolveField = ((BinaryObject)oldVal).field(conflictResolveField);
                        newResolveField = ((BinaryObject)newVal).field(conflictResolveField);
                    }
                    else {
                        oldResolveField = U.field(oldVal, conflictResolveField);
                        newResolveField = U.field(newVal, conflictResolveField);
                    }

                    return oldResolveField.compareTo(newResolveField) < 0;
                }
                catch (Exception e) {
                    log.error(
                        "Error while resolving replication conflict. [field=" + conflictResolveField + ", key=" + newEntry.key() + ']',
                        e
                    );
                }
            }
        }

        log.error("Conflict can't be resolved, update ignored [key=" + newEntry.key() + ", fromCluster=" + newEntry.dataCenterId()
            + ", toCluster=" + oldEntry.dataCenterId() + ']');

        // Ignoring update.
        return false;
    }
}
