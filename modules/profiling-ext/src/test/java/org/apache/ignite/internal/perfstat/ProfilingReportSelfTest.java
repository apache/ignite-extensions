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

package org.apache.ignite.internal.perfstat;

import java.io.File;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERFORMANCE_STAT_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the profiling report.
 */
public class ProfilingReportSelfTest {
    /** @throws Exception If failed. */
    @Test
    public void testCreateReport() throws Exception {
        try (
            Ignite srv = Ignition.start(new IgniteConfiguration().setIgniteInstanceName("srv"));

            IgniteEx client = (IgniteEx)Ignition.start(new IgniteConfiguration()
                .setIgniteInstanceName("client")
                .setClientMode(true))
        ) {
            client.context().performanceStatistics().startCollectStatistics();

            IgniteCache<Object, Object> cache = client.createCache("cache");

            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            client.context().performanceStatistics().stopCollectStatistics();

            U.sleep(1000);

            File prfDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), PERFORMANCE_STAT_DIR, false);

            assertTrue(prfDir.exists());

            PerformanceReportBuilder.main(prfDir.getAbsolutePath());

            File[] reportDir = prfDir.listFiles((dir, name) -> name.startsWith("report"));

            assertEquals(1, reportDir.length);

            File report = reportDir[0];

            File index = new File(report.getAbsolutePath() + File.separatorChar + "index.html");
            File dataDir = new File(report.getAbsolutePath() + File.separatorChar + "data");
            File dataJs = new File(dataDir.getAbsolutePath() + File.separatorChar + "data.json.js");

            assertTrue(index.exists());
            assertTrue(dataDir.exists());
            assertTrue(dataJs.exists());
        } finally {
//            U.delete(new File(U.defaultWorkDirectory()));
        }
    }
}
