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

import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntry;

/** */
public class GridCacheVersionConflictContextEx<K, V> extends GridCacheVersionConflictContext<K, V> {
    /** */
    private GridCacheVersionedEntry<K, V> entryForExpire;

    /**
     * Constructor.
     *
     * @param ctx Context to get value of cache object.
     * @param oldEntry Old entry.
     * @param newEntry New entry.
     */
    public GridCacheVersionConflictContextEx(
        CacheObjectValueContext ctx,
        GridCacheVersionedEntry<K, V> oldEntry,
        GridCacheVersionedEntry<K, V> newEntry
    ) {
        super(ctx, oldEntry, newEntry);
    }

    /** */
    public void useMaxExpireTime() {
        entryForExpire = newEntry().expireTime() > oldEntry().expireTime() ? newEntry() : oldEntry();
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return entryForExpire == null ? super.ttl() : entryForExpire.ttl();
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return entryForExpire == null ? super.expireTime() : entryForExpire.expireTime();
    }
}
