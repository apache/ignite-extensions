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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.performancestatistics.handlers.QueryHandler;
import org.apache.ignite.internal.performancestatistics.handlers.SystemViewHandler;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.waitForStatisticsEnabled;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERF_STAT_DIR;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.junits.GridAbstractTest.LOCAL_IP_FINDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the performance statistics report.
 */
public class PerformanceStatisticsReportSelfTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** @throws Exception If failed. */
    @Test
    public void testCreateReport() throws Exception {
        try (
            Ignite srv = Ignition.start(new IgniteConfiguration().setIgniteInstanceName("srv")
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(LOCAL_IP_FINDER)));

            IgniteEx client = (IgniteEx)Ignition.start(new IgniteConfiguration()
                .setIgniteInstanceName("client")
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(LOCAL_IP_FINDER))
                .setClientMode(true))
        ) {
            client.context().performanceStatistics().startCollectStatistics();

            IgniteCache<Object, Object> cache = client.createCache(new CacheConfiguration<>(CACHE_NAME)
                .setQueryEntities(F.asList(new QueryEntity()
                    .setKeyType(Integer.class.getName())
                    .setValueType(Integer.class.getName()))));

            cache.put(0, 0);
            cache.put(1, 1);
            cache.get(1);
            cache.remove(1);
            cache.putAll(Collections.singletonMap(2, 2));
            cache.getAll(Collections.singleton(2));
            cache.removeAll(Collections.singleton(2));
            cache.getAndPut(3, 3);
            cache.getAndRemove(3);

            IgniteInternalCache<Object, Object> cachex = client.cachex(CACHE_NAME);

            KeyCacheObject keyConfl = new KeyCacheObjectImpl(1, null, 1);
            CacheObject valConfl = new CacheObjectImpl(1, null);
            GridCacheVersion confl = new GridCacheVersion(1, 0, 1, (byte)2);

            cachex.putAllConflict(singletonMap(keyConfl, new GridCacheDrInfo(valConfl, confl)));
            cachex.removeAllConflict(singletonMap(keyConfl, confl));

            client.compute().run(() -> {
                // No-op.
            });

            assertThrowsWithCause(() -> client.compute().execute(new TaskWithoutJobs(), null), IgniteException.class);

            IgniteCache<Object, Object> txCache = client.createCache(new CacheConfiguration<>("txCache")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            try (Transaction tx = client.transactions().txStart()) {
                txCache.put(1, 1);

                tx.commit();
            }

            try (Transaction tx = client.transactions().txStart()) {
                txCache.put(2, 2);

                tx.rollback();
            }

            cache.query(new ScanQuery<>((key, val) -> true)).getAll();

            cache.query(new SqlFieldsQuery("select * from sys.tables").setEnforceJoinOrder(true)).getAll();

            for (int i = 0; i < 100; i++)
                cache.query(new SqlFieldsQuery("select sum(_VAL) from \"cache\".Integer")).getAll();

            cache.query(new IndexQuery<>(Integer.class).setCriteria(gt("_KEY", 0))).getAll();

            client.context().performanceStatistics().stopCollectStatistics();

            waitForStatisticsEnabled(false);

            File prfDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), PERF_STAT_DIR, false);

            assertTrue(prfDir.exists());

            PerformanceStatisticsReportBuilder.main(prfDir.getAbsolutePath());

            File[] reportDir = prfDir.listFiles((dir, name) -> name.startsWith("report"));

            assertEquals(1, reportDir.length);

            File report = reportDir[0];

            File idx = new File(report.getAbsolutePath() + File.separatorChar + "index.html");
            File dataDir = new File(report.getAbsolutePath() + File.separatorChar + "data");
            File dataJs = new File(dataDir.getAbsolutePath() + File.separatorChar + "data.json.js");

            assertTrue(idx.exists());
            assertTrue(dataDir.exists());
            assertTrue(dataJs.exists());
        }
        finally {
            U.delete(new File(U.defaultWorkDirectory()));
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testQueryHandlerAggregation() throws Exception {
        QueryHandler qryHnd = new QueryHandler();

        for (int nodeIdx = 0; nodeIdx < 10; nodeIdx++) {
            UUID nodeId = new UUID(0, nodeIdx);

            for (long id = 0; id < 1000; id++) {
                String text = "query" + (id / 100);
                UUID origNodeId = new UUID(0, id % 10);
                qryHnd.queryReads(nodeId, GridCacheQueryType.SQL_FIELDS, origNodeId, id, 1, 1);
                qryHnd.queryRows(nodeId, GridCacheQueryType.SQL_FIELDS, origNodeId, id, "ROWS", 1);
                qryHnd.queryRows(nodeId, GridCacheQueryType.SQL_FIELDS, origNodeId, id, "ROWSx2", 2);
                qryHnd.queryProperty(nodeId, GridCacheQueryType.SQL_FIELDS, origNodeId, id, "prop1", "val1");
                qryHnd.queryProperty(nodeId, GridCacheQueryType.SQL_FIELDS, origNodeId, id, "prop1", "val2");
                qryHnd.queryProperty(nodeId, GridCacheQueryType.SQL_FIELDS, origNodeId, id, "prop2", "val2");
                if (nodeId.equals(origNodeId)) {
                    qryHnd.query(nodeId, GridCacheQueryType.SQL_FIELDS, text, id, 0,
                        TimeUnit.MILLISECONDS.toNanos(id), true);
                }
            }
        }

        Map<String, JsonNode> res = qryHnd.results();
        JsonNode aggrSql = res.get("sql");
        assertEquals(10, aggrSql.size());

        for (int i = 0; i < 10; i++) {
            JsonNode aggrQry = aggrSql.get("query" + i);
            assertNotNull(aggrQry);
            assertEquals(100, aggrQry.get("count").asInt());

            assertEquals(1000, aggrQry.get("logicalReads").asInt());
            assertEquals(1000, aggrQry.get("physicalReads").asInt());

            JsonNode props = aggrQry.get("properties");
            assertNotNull(props);
            assertEquals(3, props.size());
            for (int j = 0; j < 3; j++) {
                JsonNode prop = props.get(j);
                assertNotNull(prop);
                assertTrue(prop.get("name").asText().startsWith("prop"));
                assertTrue(prop.get("value").asText().startsWith("val"));
                assertEquals(1000, prop.get("count").asInt());
            }

            JsonNode rows = aggrQry.get("rows");
            assertNotNull(rows);
            assertEquals(1000, rows.get("ROWS").asInt());
            assertEquals(2000, rows.get("ROWSx2").asInt());
        }

        JsonNode slowSql = res.get("topSlowSql");
        assertEquals(30, slowSql.size());

        for (int i = 0; i < 30; i++) {
            JsonNode slowQry = slowSql.get(i);
            assertNotNull(slowQry);
            assertEquals(10, slowQry.get("logicalReads").asInt());
            assertEquals(10, slowQry.get("physicalReads").asInt());

            JsonNode props = slowQry.get("properties");
            assertNotNull(props);
            assertEquals(3, props.size());
            for (int j = 0; j < 3; j++) {
                JsonNode prop = props.get(j);
                assertNotNull(prop);
                assertTrue(prop.get("name").asText().startsWith("prop"));
                assertTrue(prop.get("value").asText().startsWith("val"));
                assertEquals(10, prop.get("count").asInt());
            }

            JsonNode rows = slowQry.get("rows");
            assertNotNull(rows);
            assertEquals(10, rows.get("ROWS").asInt());
            assertEquals(20, rows.get("ROWSx2").asInt());
        }
    }

    /** */
    @Test
    public void testSystemViewHandler() {
        SystemViewHandler sysViewHandler = new SystemViewHandler();

        int nodesNumber = 10;
        int viewsNumber = 10;

        List<String> schema = new ArrayList<>();
        schema.add("col1");
        schema.add("col2");

        for (int id = 0; id < nodesNumber; id++) {
            UUID nodeId = new UUID(0, id);

            for (int i = 0; i < viewsNumber; i++) {
                List<Object> data = new ArrayList<>();
                data.add(i);
                data.add(i);

                sysViewHandler.systemView(nodeId, "view" + i, schema, data);
            }
        }

        JsonNode res = sysViewHandler.results().get("systemView");

        for (int id = 0; id < nodesNumber; id++) {
            UUID nodeId = new UUID(0, id);

            JsonNode nodeRes = res.get(nodeId.toString());

            for (int i = 0; i < viewsNumber; i++) {
                ObjectNode view = (ObjectNode)nodeRes.get("view" + i);

                ArrayNode schemaNode = (ArrayNode)view.get("schema");

                assertEquals(schemaNode.get(0).asText(), schema.get(0));
                assertEquals(schemaNode.get(1).asText(), schema.get(1));

                ArrayNode rowNode = (ArrayNode)view.get("data").get(0);

                assertEquals(Integer.toString(i), rowNode.get(0).asText());
                assertEquals(Integer.toString(i), rowNode.get(1).asText());
            }
        }
    }

    /** */
    private static class TaskWithoutJobs extends ComputeTaskAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
            List subgrid,
            @Nullable Object arg
        ) throws IgniteException {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List list) throws IgniteException {
            return null;
        }
    }
}
