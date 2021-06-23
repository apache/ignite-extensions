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
import java.util.Collections;
import java.util.List;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class CdcIgniteToIgniteReplicationTest extends AbstractReplicationTest {
    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActivePassiveCdc() {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < srcCluster.length; i++)
            futs.add(igniteToIgnite(srcCluster[i].configuration(), destClusterCliCfg[i], ACTIVE_PASSIVE_CACHE));

        return futs;
    }

    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActiveActiveCdc() {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < srcCluster.length; i++)
            futs.add(igniteToIgnite(srcCluster[i].configuration(), destClusterCliCfg[i], ACTIVE_ACTIVE_CACHE));

        for (int i = 0; i < destCluster.length; i++)
            futs.add(igniteToIgnite(destCluster[i].configuration(), srcClusterCliCfg[i], ACTIVE_ACTIVE_CACHE));

        return futs;
    }

    /**
     * @param srcCfg Ignite source node configuration.
     * @param destCfg Ignite destination cluster configuration.
     * @param cache Cache name to stream to kafka.
     * @return Future for Change Data Capture application.
     */
    protected IgniteInternalFuture<?> igniteToIgnite(IgniteConfiguration srcCfg, IgniteConfiguration destCfg, String cache) {
        return runAsync(() -> {
            CdcConfiguration cdcCfg = new CdcConfiguration();

            cdcCfg.setConsumer(new IgniteToIgniteCdcStreamer(destCfg, false, Collections.singleton(cache), KEYS_CNT));

            new CdcMain(srcCfg, null, cdcCfg).run();
        });
    }
}
