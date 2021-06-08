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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cdc.CaptureDataChangeReplicationTest.Data;
import org.apache.ignite.cdc.conflictplugin.CacheVersionConflictResolverPluginProvider;
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
import static org.apache.ignite.cdc.CaptureDataChangeReplicationTest.checkCRC;
import static org.apache.ignite.cdc.CaptureDataChangeReplicationTest.generateSingleData;

/**
 * Cache conflict operations test.
 */
@RunWith(Parameterized.class)
public class CacheConflictOperationsTest extends GridCommonAbstractTest {
    /** Cache mode. */
    @Parameterized.Parameter
    public CacheAtomicityMode cacheMode;

    /** Cluster id. */
    @Parameterized.Parameter(1)
    public byte clusterId;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "cacheMode={0}, clusterId={1}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
            {ATOMIC, THIRD_CLUSTER},
            {TRANSACTIONAL, THIRD_CLUSTER},
            {ATOMIC, FIRST_CLUSTER},
            {TRANSACTIONAL, FIRST_CLUSTER},
        });
    }

    /** */
    private static IgniteCache<String, Data> cache;

    /** */
    private static IgniteInternalCache<BinaryObject, BinaryObject> cachex;

    /** */
    private static IgniteEx cli;

    /** Cluster have a greater priority that {@link #SECOND_CLUSTER}. */
    private static final byte FIRST_CLUSTER = 1;

    /** */
    private static final byte SECOND_CLUSTER = 2;

    /** Cluster have a lower priority that {@link #SECOND_CLUSTER}. */
    private static final byte THIRD_CLUSTER = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheVersionConflictResolverPluginProvider<?> pluginCfg = new CacheVersionConflictResolverPluginProvider<>();

        pluginCfg.setClusterId(SECOND_CLUSTER);
        pluginCfg.setCaches(new HashSet<>(Collections.singleton(DEFAULT_CACHE_NAME)));
        pluginCfg.setConflictResolveField(conflictResolveField());

        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(pluginCfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1);
        cli = startClientGrid(2);

        cache = cli.createCache(new CacheConfiguration<String, Data>(DEFAULT_CACHE_NAME).setAtomicityMode(cacheMode));
        cachex = cli.cachex(DEFAULT_CACHE_NAME);
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

        putx(key(key, clusterId), clusterId, 1, true);

        putx(key(key, clusterId), clusterId, 2, true);

        removex(key(key, clusterId), clusterId, 3, true);
    }


    /**
     * Tests that {@code IgniteInternalCache#*AllConflict} cache operations works with the conflict resolver
     * when there is no update conflicts.
     */
    @Test
    public void testUpdatesReorderFromOtherCluster() throws Exception {
        String key = "UpdateClusterUpdateReorder";

        putx(key(key, clusterId), clusterId, 2, true);

        // Update with the equal or lower order should fail.
        putx(key(key, clusterId), clusterId, 2, false);
        putx(key(key, clusterId), clusterId, 1, false);

        // Remove with the equal or lower order should fail.
        removex(key(key, clusterId), clusterId, 2, false);
        removex(key(key, clusterId), clusterId, 1, false);

        // Remove with the higher order should succeed.
        putx(key(key, clusterId), clusterId, 3, true);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict() throws Exception {
        String key = "UpdateThisClusterConflict0";

        putx(key(key, clusterId), clusterId, 1, true);

        // Local remove for other cluster entry should succeed.
        remove(key(key, clusterId));

        // Conflict replicated update succeed only if cluster has a greater priority than this cluster.
        putx(key(key, clusterId), clusterId, 2, clusterId == FIRST_CLUSTER);

        key = "UpdateThisDCConflict1";

        putx(key(key, clusterId), clusterId, 3, true);

        // Local update for other cluster entry should succeed.
        put(key(key, clusterId));

        key = "UpdateThisDCConflict2";

        put(key(key, clusterId));

        // Conflict replicated remove succeed only if DC has a greater priority than this DC.
        removex(key(key, clusterId), clusterId, 4, clusterId == FIRST_CLUSTER);

        key = "UpdateThisDCConflict3";

        put(key(key, clusterId));

        // Conflict replicated update succeed only if DC has a greater priority than this DC.
        putx(key(key, clusterId), clusterId, 5, clusterId == FIRST_CLUSTER || conflictResolveField() != null);
    }

    /** */
    private void put(String key) {
        Data newVal = generateSingleData(1);

        cache.put(key, newVal);

        assertEquals(newVal, cache.get(key));

        checkCRC(cache.get(key), 1);
    }

    /** */
    private void putx(String k, byte clusterId, long order, boolean expectSuccess) throws IgniteCheckedException {
        Data oldVal = cache.get(k);
        Data newVal = generateSingleData(1);

        KeyCacheObject key = new KeyCacheObjectImpl(k, null, cachex.context().affinity().partition(k));
        CacheObject val = new CacheObjectImpl(cli.binary().toBinary(newVal), null);

        cachex.putAllConflict(singletonMap(key, new GridCacheDrInfo(val, new GridCacheVersion(1, order, 1, clusterId))));

        if (expectSuccess) {
            assertTrue(cache.containsKey(k));

            assertEquals(newVal, cache.get(k));

            checkCRC(cache.get(k), newVal.getIter());
        }
        else {
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
    private void removex(String k, byte clusterId, long order, boolean expectSuccess) throws IgniteCheckedException {
        Data oldVal = cache.get(k);

        KeyCacheObject key = new KeyCacheObjectImpl(k, null, cachex.context().affinity().partition(k));

        cachex.removeAllConflict(singletonMap(key, new GridCacheVersion(1, order, 1, clusterId)));

        if (expectSuccess)
            assertFalse(cache.containsKey(k));
        else {
            assertTrue(cache.containsKey(k) || oldVal == null);

            if (oldVal != null)
                assertEquals(oldVal, cache.get(k));
        }
    }

    /** */
    private String key(String key, byte clusterId) {
        return key + clusterId + cacheMode;
    }

    /** */
    protected String conflictResolveField() {
        return null;
    }
}