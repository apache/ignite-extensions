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

package org.apache.ignite.internal.performancestatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.waitForStatisticsEnabled;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERF_STAT_DIR;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_PUT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_START;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TASK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_COMMIT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_ROLLBACK;
import static org.junit.Assert.assertTrue;

/**
 * Tests the performance statistics reader.
 */
public class PerformanceStatisticsReaderTest {
    /** @throws Exception If failed. */
    @Test
    public void testReadToFile() throws Exception {
        try (
            Ignite srv = Ignition.start(new IgniteConfiguration().setIgniteInstanceName("srv"));

            IgniteEx client = (IgniteEx)Ignition.start(new IgniteConfiguration()
                .setIgniteInstanceName("client")
                .setClientMode(true))
        ) {
            client.context().performanceStatistics().startCollectStatistics();

            IgniteCache<Object, Object> cache = client.createCache(new CacheConfiguration<>("txCache")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            try (Transaction tx = client.transactions().txStart()) {
                cache.put(1, 1);

                tx.commit();
            }

            try (Transaction tx = client.transactions().txStart()) {
                cache.put(2, 2);

                tx.rollback();
            }

            client.compute().run(() -> {
                // No-op.
            });

            cache.query(new ScanQuery<>((key, val) -> true)).getAll();
            cache.query(new SqlFieldsQuery("select * \nfrom sys.tables")).getAll();

            client.context().performanceStatistics().stopCollectStatistics();

            waitForStatisticsEnabled(false);

            File prfDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), PERF_STAT_DIR, false);

            assertTrue(prfDir.exists());

            File out = new File(U.defaultWorkDirectory(), "report.txt");

            PerformanceStatisticsReader.main(prfDir.getAbsolutePath(),
                "--out", out.getAbsolutePath());

            assertTrue(out.exists());

            Set<OperationType> expOp = new HashSet<>(Arrays.asList(CACHE_START, CACHE_PUT, TASK, JOB,
                TX_COMMIT, TX_ROLLBACK, QUERY, QUERY_READS));

            ObjectMapper mapper = new ObjectMapper();

            try (BufferedReader reader = new BufferedReader(new FileReader(out))) {
                String line;

                while ((line = reader.readLine()) != null) {
                    JsonNode json = mapper.readTree(line);

                    assertTrue(json.isObject());

                    OperationType op = OperationType.valueOf(json.get("op").asText());

                    expOp.remove(op);

                    UUID nodeId = UUID.fromString(json.get("nodeId").asText());

                    assertTrue(F.nodeIds(srv.cluster().nodes()).contains(nodeId));
                }
            }

            assertTrue("Expected operations:" + expOp, expOp.isEmpty());
        } finally {
            U.delete(new File(U.defaultWorkDirectory()));
        }
    }
}
