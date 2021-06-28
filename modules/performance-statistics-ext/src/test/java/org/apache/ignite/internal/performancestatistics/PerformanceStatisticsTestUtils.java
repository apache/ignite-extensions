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

import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.testframework.junits.GridTestKernalContext;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class PerformanceStatisticsTestUtils {
    /** Test node ID. */
    public static final UUID TEST_NODE_ID = UUID.randomUUID();

    /** Writes statistics through passed writer. */
    public static void createStatistics(Consumer<FilePerformanceStatisticsWriter> c) throws Exception {
        createStatistics(TEST_NODE_ID, c);
    }

    /** Writes statistics through passed writer. */
    public static void createStatistics(UUID nodeId, Consumer<FilePerformanceStatisticsWriter> c) throws Exception {
        FilePerformanceStatisticsWriter writer = new FilePerformanceStatisticsWriter(new TestKernalContext(nodeId));

        writer.start();

        waitForCondition(() -> U.field((Object)U.field(writer, "fileWriter"), "runner") != null, 30_000);

        c.accept(writer);

        writer.stop();
    }

    /** Test kernal context. */
    private static class TestKernalContext extends GridTestKernalContext {
        /** Node ID. */
        private final UUID nodeId;

        /** @param nodeId Node ID. */
        public TestKernalContext(UUID nodeId) {
            super(new JavaLogger());

            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public UUID localNodeId() {
            return nodeId;
        }
    }
}
