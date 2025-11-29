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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverImpl;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheTestEntryEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
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
public abstract class CacheConflictOperationsAbstractTest extends GridCommonAbstractTest {
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
    private static IgniteCache<String, ConflictResolvableTestData> cache;

    /** */
    private static IgniteInternalCache<BinaryObject, BinaryObject> cachex;

    /** */
    private static IgniteEx client;

    /** Listening test logger. */
    private static ListeningTestLogger listeningLog;

    /** */
    private static final AtomicInteger incKey = new AtomicInteger();

    /** */
    protected static volatile boolean removeAfterRemove;

    /** */
    private static final byte FIRST_CLUSTER_ID = 1;

    /** */
    private static final byte SECOND_CLUSTER_ID = 2;

    /** */
    private static final byte THIRD_CLUSTER_ID = 3;

    /** */
    protected volatile ConflictResolvableTestData prevValue;

    /** */
    protected volatile GridCacheVersion prevVersion;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheVersionConflictResolverPluginProvider<?> pluginCfg = new CacheVersionConflictResolverPluginProvider<>();

        pluginCfg.setClusterId(SECOND_CLUSTER_ID);
        pluginCfg.setCaches(new HashSet<>(Collections.singleton(DEFAULT_CACHE_NAME)));
        pluginCfg.setConflictResolveField(conflictResolveField());

        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(pluginCfg)
            .setGridLogger(listeningLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        listeningLog = new ListeningTestLogger(log);

        startGrid(1);
        client = startClientGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        cache = null;
        cachex = null;
        client = null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (cachex == null || cachex.configuration().getAtomicityMode() != cacheMode) {
            if (cachex != null)
                client.cache(DEFAULT_CACHE_NAME).destroy();

            cache = client.createCache(new CacheConfiguration<String, ConflictResolvableTestData>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(cacheMode));

            cachex = client.cachex(DEFAULT_CACHE_NAME);
        }

        assert !removeAfterRemove;
    }

    /** Test switching debug log level for ConflictResolver during runtime */
    @Test
    public void testResolveDebug() throws Exception {
        String key = nextKey();

        LogListener lsnr = LogListener.matches("isUseNew").build();

        listeningLog.registerListener(lsnr);

        try {
            Configurator.setLevel(CacheVersionConflictResolverImpl.class.getName(), Level.DEBUG);

            try {
                putFromOther(key, 1, true);

                putFromOther(key, 1, false);

                assertTrue(lsnr.check());
            }
            finally {
                Configurator.setLevel(CacheVersionConflictResolverImpl.class.getName(), Level.INFO);
            }

            lsnr.reset();

            putFromOther(key, 1, false);

            assertFalse(lsnr.check());
        }
        finally {
            listeningLog.unregisterListener(lsnr);
        }
    }

    /** */
    protected void putLocal(String key) {
        ConflictResolvableTestData newVal = ConflictResolvableTestData.create();

        CacheEntry<String, ConflictResolvableTestData> oldEntry = cache.getEntry(key);

        cache.put(key, newVal);

        CacheEntry<String, ConflictResolvableTestData> newEntry = cache.getEntry(key);

        assertNull(((CacheEntryVersion)newEntry.version()).otherClusterVersion());
        assertEquals(newVal, cache.get(key));

        if (oldEntry != null)
            assertTrue(((CacheEntryVersion)oldEntry.version()).order() < ((CacheEntryVersion)newEntry.version()).order());
    }

    /** Puts entry via {@link IgniteInternalCache#putAllConflict(Map)}. */
    protected void putFromOther(String k, boolean success) throws IgniteCheckedException {
        putFromOther(k, 1, success);
    }

    /** Puts entry via {@link IgniteInternalCache#putAllConflict(Map)}. */
    protected void putFromOther(String k, long order, boolean success) throws IgniteCheckedException {
        putFromOther(k, new GridCacheVersion(1, order, 1, otherClusterId), success);
    }

    /** Puts entry via {@link IgniteInternalCache#putAllConflict(Map)}. */
    protected void putFromOther(String k, GridCacheVersion newVer, boolean success) throws IgniteCheckedException {
        CacheEntry<String, ConflictResolvableTestData> oldEntry = cache.getEntry(k);
        ConflictResolvableTestData newVal = ConflictResolvableTestData.create();

        KeyCacheObject key = new KeyCacheObjectImpl(k, null, cachex.context().affinity().partition(k));
        CacheObject val = new CacheObjectImpl(client.binary().toBinary(newVal), null);

        CacheVersionConflictResolver resolver = cachex.context().conflictResolver();

        GridCacheEntryEx entry =
            new GridCacheTestEntryEx(cachex.context(), key, cachex.context().toCacheObject(prevValue), prevVersion, 0);

        CacheObject prevStateMeta = cachex.context().toCacheObject(resolver.previousStateMetadata(entry));

        cachex.putAllConflict(singletonMap(key, new GridCacheDrInfo(val, newVer, prevStateMeta)));

        if (success) {
            assertEquals(newVer, ((GridCacheVersion)cache.getEntry(k).version()).conflictVersion());
            assertEquals(newVal, cache.get(k));

            prevValue = newVal;
            prevVersion = newVer;
        }
        else if (oldEntry != null) {
            assertEquals(oldEntry.getValue(), cache.get(k));
            assertEquals(oldEntry.version(), cache.getEntry(k).version());
        }
    }

    /** Replicates entry to the virtual other cluster. */
    protected void replicateToOther(String k) {
        CacheEntry<String, ConflictResolvableTestData> entry = cache.getEntry(k);

        prevValue = entry != null ? entry.getValue() : null;

        prevVersion = entry != null ?
            GridCacheVersionEx.addConflictVersion(
                new GridCacheVersion(1, 1, 1, otherClusterId), (GridCacheVersion)entry.version()) :
            null;
    }

    /** */
    protected void removeLocal(String key) {
        assertTrue(removeAfterRemove ^ cache.containsKey(key));

        cache.remove(key);

        assertFalse(cache.containsKey(key));
    }

    /** Removes entry via {@link IgniteInternalCache#removeAllConflict(Map)}. */
    protected void removeFromOther(String k, boolean success) throws IgniteCheckedException {
        removeFromOther(k, 1, success);
    }

    /** Removes entry via {@link IgniteInternalCache#removeAllConflict(Map)}. */
    protected void removeFromOther(String k, long order, boolean success) throws IgniteCheckedException {
        removeFromOther(k, new GridCacheVersion(1, order, 1, otherClusterId), success);
    }

    /** Removes entry via {@link IgniteInternalCache#removeAllConflict(Map)}. */
    protected void removeFromOther(String k, GridCacheVersion ver, boolean success) throws IgniteCheckedException {
        assertTrue(removeAfterRemove ^ cache.containsKey(k));

        CacheEntry<String, ConflictResolvableTestData> oldEntry = cache.getEntry(k);

        KeyCacheObject key = new KeyCacheObjectImpl(k, null, cachex.context().affinity().partition(k));

        CacheVersionConflictResolver resolver = cachex.context().conflictResolver();

        GridCacheEntryEx entry =
            new GridCacheTestEntryEx(cachex.context(), key, cachex.context().toCacheObject(prevValue), prevVersion, 0);

        CacheObject prevStateMeta = cachex.context().toCacheObject(resolver.previousStateMetadata(entry));

        cachex.putAllConflict(singletonMap(key, new GridCacheDrInfo(null, ver, prevStateMeta)));

        if (success)
            assertFalse(cache.containsKey(k));
        else if (oldEntry != null) {
            assertEquals(oldEntry.getValue(), cache.get(k));
            assertEquals(oldEntry.version(), cache.getEntry(k).version());
        }
    }

    /** */
    protected String nextKey() {
        return "Key_" + incKey.incrementAndGet() + "_" + otherClusterId + "_" + cacheMode;
    }

    /** */
    protected String conflictResolveField() {
        return null;
    }

    /** */
    protected enum Operation {
        /** */
        NONE,

        /** */
        PUT,

        /** */
        REMOVE
    }
}
