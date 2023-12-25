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

import java.util.function.Function;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Cache conflict operations test.
 */
@RunWith(Parameterized.class)
public class CacheConflictOperationsTest extends CacheConflictOperationsAbstractTest {
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
        String key = key("UpdateFromOtherClusterWithoutConflict", otherClusterId);

        putConflict(key, 1, true);
        putConflict(key, 2, true);

        removeConflict(key, 3, true);
    }

    /**
     * Tests that {@code IgniteInternalCache#*AllConflict} cache operations works with the conflict resolver
     * when there are update conflicts.
     */
    @Test
    public void testUpdatesReorderFromOtherCluster() throws Exception {
        testUpdatesReorderFromOtherCluster(
            key("UpdateClusterUpdateReorder1", otherClusterId),
            (topVer) -> new GridCacheVersion(topVer, 1, 1, otherClusterId));

        testUpdatesReorderFromOtherCluster(
            key("UpdateClusterUpdateReorder2", otherClusterId),
            (order) -> new GridCacheVersion(1, order, 1, otherClusterId));

        testUpdatesReorderFromOtherCluster(
            key("UpdateClusterUpdateReorder3", otherClusterId),
            (nodeOrder) -> new GridCacheVersion(1, 1, nodeOrder, otherClusterId));
    }

    /** */
    private void testUpdatesReorderFromOtherCluster(String key, Function<Integer, GridCacheVersion> verGen) throws Exception {
        putConflict(key, verGen.apply(2), true);

        // Update with the equal or lower version should be ignored.
        putConflict(key, verGen.apply(2), false);
        putConflict(key, verGen.apply(1), false);

        // Remove with the equal or lower version should be ignored.
        removeConflict(key, verGen.apply(2), false);
        removeConflict(key, verGen.apply(1), false);

        // Remove with the higher order should succeed.
        putConflict(key, verGen.apply(3), true);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict1() throws Exception {
        String key = key("UpdateThisClusterConflict1", otherClusterId);

        putConflict(key, 1, true);

        // Local remove for other cluster entry should succeed.
        remove(key);

        // Conflict replicated update should be ignored.
        // Resolve by field value not applicable because after remove operation "old" value doesn't exist.
        putConflict(key, 2, false);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict1Replicated() throws Exception {
        String key = key("UpdateThisClusterConflict1Replicated", otherClusterId);

        putConflict(key, 1, true);

        // Local remove for other cluster entry should succeed.
        remove(key);

        replicate(key);

        // Conflict replicated update shouldn't be ignored.
        // Both clusters had the same state before this change.
        putConflict(key, 2, true);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict2() throws Exception {
        String key = key("UpdateThisClusterConflict2", otherClusterId);

        putConflict(key, true);

        // Local update for other cluster entry should succeed.
        put(key);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict3() throws Exception {
        String key = key("UpdateThisClusterConflict3", otherClusterId);

        put(key);

        // Conflict replicated remove should be ignored.
        removeConflict(key, false);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict3Replicated() throws Exception {
        String key = key("UpdateThisClusterConflict3Replicated", otherClusterId);

        put(key);

        replicate(key);

        // Conflict replicated update shouldn't be ignored.
        // Both clusters had the same state before this change.
        removeConflict(key, true);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict4() throws Exception {
        String key = key("UpdateThisClusterConflict4", otherClusterId);

        put(key);

        // Conflict replicated update succeed only if resolved by field.
        putConflict(key, conflictResolveField() != null);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict4Replicated() throws Exception {
        String key = key("UpdateThisClusterConflict4Replicated", otherClusterId);

        put(key);

        replicate(key);

        // Conflict replicated update shouldn't be ignored.
        // Both clusters had the same state before this change.
        putConflict(key, true);
    }
}
