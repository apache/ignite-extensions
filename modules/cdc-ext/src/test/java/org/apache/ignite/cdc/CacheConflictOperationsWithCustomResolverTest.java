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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** Cache conflict operations test with a custom resolver. */
public class CacheConflictOperationsWithCustomResolverTest extends CacheConflictOperationsAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((CacheVersionConflictResolverPluginProvider<?>)cfg.getPluginProviders()[0]).setConflictResolver(new LwwConflictResolver());

        return cfg;
    }

    /** Tests simple updates. */
    @Test
    public void testSimpleUpdates() {
        String key = nextKey();

        putLocal(key);
        putLocal(key);

        removeLocal(key);
    }

    /** Tests simple conflicts can be resolved with LWW. */
    @Test
    public void testSimpleConflicts() throws IgniteCheckedException {
        String key = nextKey();

        putLocal(key);

        // LWW.
        removeFromOther(key, true);
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testResolveDebug() throws Exception {
        // LWW strategy resolves conflicts in unexpected way at versioned resolve test.
        GridTestUtils.assertThrows(log, super::testResolveDebug, AssertionError.class, "");
    }

    /**
     *
     */
    private static final class LwwConflictResolver implements CacheVersionConflictResolver {
        /**
         *
         */
        @Override public <K, V> GridCacheVersionConflictContext<K, V> resolve(
            CacheObjectValueContext ctx,
            GridCacheVersionedEntryEx<K, V> oldEntry,
            GridCacheVersionedEntryEx<K, V> newEntry,
            Object prevStateMeta,
            boolean atomicVerComparator) {
            GridCacheVersionConflictContext<K, V> res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

            res.useNew();

            return res;
        }
    }
}
