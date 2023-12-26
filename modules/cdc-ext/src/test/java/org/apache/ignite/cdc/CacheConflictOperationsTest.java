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

        for (int i = 0; i < 3; i++) {
            putLocal(key);
            putLocal(key);

            removeLocal(key);
        }
    }

    /**
     * Tests that {@code IgniteInternalCache#*AllConflict} cache operations works with the conflict resolver
     * when there is no update conflicts.
     */
    @Test
    public void testUpdatesFromOtherClusterWithoutConflict() throws Exception {
        String key = key("UpdateFromOtherClusterWithoutConflict", otherClusterId);

        putFromOther(key, 1, true);
        putFromOther(key, 2, true);

        removeFromOther(key, 3, true);

        putFromOther(key, 4, true);
        putFromOther(key, 5, true);

        removeFromOther(key, 6, true);
    }

    /**
     * Tests that {@code IgniteInternalCache#*AllConflict} cache operations works with the conflict resolver
     * when there are update conflicts.
     */
    @Test
    public void testUpdatesFromOtherClusterWithConflict() throws Exception {
        String key = key("UpdateFromOtherClusterWithConflict", otherClusterId);

        putFromOther(key, 1, true);
        putFromOther(key, 2, true);

        removeFromOther(key, 3, true);

        putFromOther(key, 3, false);
        putFromOther(key, 4, true);
        putFromOther(key, 4, false);
        putFromOther(key, 4, false);

        removeFromOther(key, 3, false);

        putFromOther(key, 4, false);

        removeFromOther(key, 4, false);
        removeFromOther(key, 5, true);
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
        putFromOther(key, verGen.apply(2), true);

        for (int i = 0; i < 3; i++) {
            // Update with the equal or lower version should be ignored.
            putFromOther(key, verGen.apply(2), false);
            putFromOther(key, verGen.apply(1), false);

            // Remove with the equal or lower version should be ignored.
            removeFromOther(key, verGen.apply(2), false);
            removeFromOther(key, verGen.apply(1), false);
        }

        // Remove with the higher order should succeed.
        putFromOther(key, verGen.apply(3), true);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict() throws Exception {
        String key = key("UpdateThisClusterConflict", otherClusterId);

        putFromOther(key, true);

        // Local update for other cluster entry should succeed.
        putLocal(key);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict00() throws Exception {
        testUpdatesConflict00(false);
        testUpdatesConflict00(true);
    }

    /** */
    private void testUpdatesConflict00(boolean replication) throws Exception {
        String key = key("UpdateThisClusterConflict00" + (replication ? "Replicated" : ""), otherClusterId);

        putFromOther(key, 1, true);

        // Local remove for other cluster entry should succeed.
        putLocal(key);

        if (replication)
            replicateToOther(key);

        // Non-replicated:
        // Conflict replicated update should be ignored.
        // Resolve by field value not applicable because after remove operation "old" value doesn't exist.

        // Replicated:
        // Conflict replicated update shouldn't be ignored.
        // Both clusters had the same state before this change.
        removeFromOther(key, 2, replication);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict0() throws Exception {
        testUpdatesConflict0(false);
        testUpdatesConflict0(true);
    }

    /** */
    private void testUpdatesConflict0(boolean replication) throws Exception {
        String key = key("UpdateThisClusterConflict0" + (replication ? "Replicated" : ""), otherClusterId);

        putFromOther(key, 1, true);

        // Local remove for other cluster entry should succeed.
        putLocal(key);

        if (replication)
            replicateToOther(key);

        // Non-replicated:
        // Conflict replicated update should be ignored.
        // Resolve by field value not applicable because after remove operation "old" value doesn't exist.

        // Replicated:
        // Conflict replicated update shouldn't be ignored.
        // Both clusters had the same state before this change.
        putFromOther(key, 2, replication || conflictResolveField() != null);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict1() throws Exception {
        testUpdatesConflict1(false);
        testUpdatesConflict1(true);
    }

    /** */
    private void testUpdatesConflict1(boolean replication) throws Exception {
        String key = key("UpdateThisClusterConflict1" + (replication ? "Replicated" : ""), otherClusterId);

        putFromOther(key, 1, true);

        // Local remove for other cluster entry should succeed.
        removeLocal(key);

        if (replication)
            replicateToOther(key);

        // Non-replicated:
        // Conflict replicated update should be ignored.
        // Resolve by field value not applicable because after remove operation "old" value doesn't exist.

        // Replicated:
        // Conflict replicated update shouldn't be ignored.
        // Both clusters had the same state before this change.
        putFromOther(key, 2, replication);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict3() throws Exception {
        testUpdatesConflict3(false);
        testUpdatesConflict3(true);
    }

    /** */
    private void testUpdatesConflict3(boolean replication) throws Exception {
        String key = key("UpdateThisClusterConflict3" + (replication ? "Replicated" : ""), otherClusterId);

        putLocal(key);

        if (replication)
            replicateToOther(key);

        // Non-replicated:
        // Conflict replicated remove should be ignored.

        // Replicated:
        // Conflict replicated update shouldn't be ignored.
        // Both clusters had the same state before this change.
        removeFromOther(key, replication);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict4() throws Exception {
        testUpdatesConflict4(false);
        testUpdatesConflict4(true);
    }

    /** */
    private void testUpdatesConflict4(boolean replication) throws Exception {
        String key = key("UpdateThisClusterConflict4" + (replication ? "Replicated" : ""), otherClusterId);

        putLocal(key);

        if (replication)
            replicateToOther(key);

        // Non-replicated:
        // Conflict replicated update succeed only if resolved by field.

        // Replicated:
        // Conflict replicated update shouldn't be ignored.
        // Both clusters had the same state before this change.
        putFromOther(key, replication || conflictResolveField() != null);
    }
}
