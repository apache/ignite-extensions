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
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cdc.CacheConflictOperationsAbstractTest.Operation.NONE;
import static org.apache.ignite.cdc.CacheConflictOperationsAbstractTest.Operation.PUT;
import static org.apache.ignite.cdc.CacheConflictOperationsAbstractTest.Operation.REMOVE;

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
        String key = nextKey();

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
        String key = nextKey();

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
            nextKey(),
            (topVer) -> new GridCacheVersion(topVer, 1, 1, otherClusterId));

        testUpdatesReorderFromOtherCluster(
            nextKey(),
            (order) -> new GridCacheVersion(1, order, 1, otherClusterId));

        testUpdatesReorderFromOtherCluster(
            nextKey(),
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
    public void testLocalUpdateWins() throws Exception {
        String key = nextKey();

        putFromOther(key, true);

        // Local update for other cluster entry should succeed.
        putLocal(key);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict() throws Exception {
        int cnt = 0;

        for (Operation op1 : operations(NONE, PUT)) { // From other cluster.
            for (Operation op2 : operations(PUT, op1 == PUT ? REMOVE : null)) { // Local.
                for (Operation op3 : operations(PUT, op2 == PUT ? REMOVE : null)) { // From other cluster.
                    for (boolean replication : new boolean[] {true, false})
                        testUpdatesConflict(op1, op2, op3, replication);

                    cnt++;
                }
            }
        }

        // Starting from local:
        //  none -> put -> put
        //  none -> put -> remove
        // Starting from other cluster:
        //  put -> put -> put
        //  put -> put -> remove
        //  put -> remove -> put
        assert cnt == 5 : cnt; // Refactoring checker.
    }

    private Operation[] operations(Operation... ops) {
        List<Operation> opList = new ArrayList<>();

        for (Operation op : ops) {
            if (op != null)
                opList.add(op);
        }

        return opList.toArray(new Operation[0]);
    }

    /** */
    private void testUpdatesConflict(Operation op1, Operation op2, Operation op3, boolean replication) throws Exception {
        log.info("Checking: " + op1 + ", " + op2 + ", " + op3 + ", replication=" + replication);

        String key = nextKey();

        if (op1 == PUT)
            putFromOther(key, 1, true);
        else
            assert op1 == NONE;

        if (op2 == PUT)
            // Local remove for other cluster entry should succeed.
            putLocal(key);
        else {
            assert op2 == REMOVE;

            // Local remove for other cluster entry should succeed.
            removeLocal(key);
        }

        if (replication)
            replicateToOther(key);

        // Update is always successful when replication is finished and both clusters have the same state.
        boolean success = replication;

        if (op2 != REMOVE && op3 != REMOVE) {
            // Values can be compared via the field when
            // - previous value exist (was created and was not removed)
            // - new value contain field (not a remove).
            // Update is successful when can be resolved by field.
            success |= conflictResolveField() != null;
        }

        if (op3 == PUT)
            putFromOther(key, success);
        else {
            assert op3 == REMOVE;

            removeFromOther(key, 2, success);
        }
    }
}
