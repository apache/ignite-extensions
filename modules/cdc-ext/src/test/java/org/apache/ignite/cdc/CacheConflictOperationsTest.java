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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cdc.kafka.Data;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Cache conflict operations test.
 */
@RunWith(Parameterized.class)
public class CacheConflictOperationsTest extends GridCommonAbstractTest {
    /** Cache mode. */
    @Parameterized.Parameter
    public CacheAtomicityMode cacheMode;

    /** Other cluster id. */
    @Parameterized.Parameter(1)
    public byte otherClusterId;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "cacheMode={0}, otherClusterId={1}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode mode : EnumSet.of(ATOMIC, TRANSACTIONAL))
            for (byte otherClusterId : new byte[] {FIRST_CLUSTER_ID, THIRD_CLUSTER_ID})
                params.add(new Object[] {mode, otherClusterId});

        return params;
    }

    /** */
    private static IgniteCache<String, Data> cache;

    /** */
    private static IgniteInternalCache<BinaryObject, BinaryObject> cachex;

    /** */
    private static IgniteEx client;

    /** Cluster have a greater priority that {@link #SECOND_CLUSTER_ID}. */
    private static final byte FIRST_CLUSTER_ID = 1;

    /** */
    private static final byte SECOND_CLUSTER_ID = 2;

    /** Cluster have a lower priority that {@link #SECOND_CLUSTER_ID}. */
    private static final byte THIRD_CLUSTER_ID = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheVersionConflictResolverPluginProvider<?> pluginCfg = new CacheVersionConflictResolverPluginProvider<>();

        pluginCfg.setClusterId(SECOND_CLUSTER_ID);
        pluginCfg.setCaches(new HashSet<>(Collections.singleton(DEFAULT_CACHE_NAME)));
        pluginCfg.setConflictResolveField(conflictResolveField());

        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(pluginCfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1);
        client = startClientGrid(2);

        cache = client.createCache(new CacheConfiguration<String, Data>(DEFAULT_CACHE_NAME).setAtomicityMode(cacheMode));
        cachex = client.cachex(DEFAULT_CACHE_NAME);
    }

    /** Tests that regular cache operations works with the conflict resolver when there is no update conflicts. */
    @Test
    public void testSimpleUpdates() {
        String key = "UpdatesWithoutConflict";

        put(key);

        put(key);

        remove(key);
    }

    /**
     * Tests that {@code IgniteInternalCache#*AllConflict} cache operations works with the conflict resolver
     * when there is no update conflicts.
     */
    @Test
    public void testUpdatesFromOtherClusterWithoutConflict() throws Exception {
        String key = "UpdateFromOtherClusterWithoutConflict";

        putx(key(key, otherClusterId), otherClusterId, 1, true);

        putx(key(key, otherClusterId), otherClusterId, 2, true);

        removex(key(key, otherClusterId), otherClusterId, 3, true);
    }


    /**
     * Tests that {@code IgniteInternalCache#*AllConflict} cache operations works with the conflict resolver
     * when there is no update conflicts.
     */
    @Test
    public void testUpdatesReorderFromOtherCluster() throws Exception {
        String key = "UpdateClusterUpdateReorder";

        putx(key(key, otherClusterId), otherClusterId, 2, true);

        // Update with the equal or lower order should fail.
        putx(key(key, otherClusterId), otherClusterId, 2, false);
        putx(key(key, otherClusterId), otherClusterId, 1, false);

        // Remove with the equal or lower order should fail.
        removex(key(key, otherClusterId), otherClusterId, 2, false);
        removex(key(key, otherClusterId), otherClusterId, 1, false);

        // Remove with the higher order should succeed.
        putx(key(key, otherClusterId), otherClusterId, 3, true);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict() throws Exception {
        String key = "UpdateThisClusterConflict0";

        putx(key(key, otherClusterId), otherClusterId, 1, true);

        // Local remove for other cluster entry should succeed.
        remove(key(key, otherClusterId));

        // Conflict replicated update succeed only if cluster has a greater priority than this cluster.
        putx(key(key, otherClusterId), otherClusterId, 2, otherClusterId == FIRST_CLUSTER_ID);

        key = "UpdateThisDCConflict1";

        putx(key(key, otherClusterId), otherClusterId, 3, true);

        // Local update for other cluster entry should succeed.
        put(key(key, otherClusterId));

        key = "UpdateThisDCConflict2";

        put(key(key, otherClusterId));

        // Conflict replicated remove succeed only if DC has a greater priority than this DC.
        removex(key(key, otherClusterId), otherClusterId, 4, otherClusterId == FIRST_CLUSTER_ID);

        key = "UpdateThisDCConflict3";

        put(key(key, otherClusterId));

        // Conflict replicated update succeed only if DC has a greater priority than this DC.
        putx(key(key, otherClusterId), otherClusterId, 5, otherClusterId == FIRST_CLUSTER_ID || conflictResolveField() != null);
    }

    /** */
    private void put(String key) {
        Data newVal = Data.create();

        cache.put(key, newVal);

        assertEquals(newVal, cache.get(key));
    }

    /** */
    private void putx(String k, byte otherClusterId, long order, boolean expectSuccess) throws IgniteCheckedException {
        Data oldVal = cache.get(k);
        Data newVal = Data.create();

        KeyCacheObject key = new KeyCacheObjectImpl(k, null, cachex.context().affinity().partition(k));
        CacheObject val = new CacheObjectImpl(client.binary().toBinary(newVal), null);

        GridCacheVersion ver = new GridCacheVersion(1, order, 1, otherClusterId);

        cachex.putAllConflict(singletonMap(key, new GridCacheDrInfo(val, ver)));

        if (expectSuccess) {
            assertEquals(ver, ((CacheEntryVersion)cache.getEntry(k).version()).otherClusterVersion());
            assertEquals(newVal, cache.get(k));
        } else {
            assertTrue(cache.containsKey(k) || oldVal == null);

            if (oldVal != null)
                assertEquals(oldVal, cache.get(k));
        }
    }

    /** */
    private void remove(String key) {
        cache.remove(key);

        assertFalse(cache.containsKey(key));
    }

    /** */
    private void removex(String k, byte otherClusterId, long order, boolean expectSuccess) throws IgniteCheckedException {
        Data oldVal = cache.get(k);

        KeyCacheObject key = new KeyCacheObjectImpl(k, null, cachex.context().affinity().partition(k));

        cachex.removeAllConflict(singletonMap(key, new GridCacheVersion(1, order, 1, otherClusterId)));

        if (expectSuccess)
            assertFalse(cache.containsKey(k));
        else {
            assertTrue(cache.containsKey(k) || oldVal == null);

            if (oldVal != null)
                assertEquals(oldVal, cache.get(k));
        }
    }

    /** */
    private String key(String key, byte otherClusterId) {
        return key + otherClusterId + cacheMode;
    }

    /** */
    protected String conflictResolveField() {
        return null;
    }
}
