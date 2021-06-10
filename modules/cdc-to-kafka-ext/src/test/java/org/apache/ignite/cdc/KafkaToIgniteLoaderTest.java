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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cdc.KafkaToIgniteLoader.loadKafkaToIgniteStreamer;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** Tests load {@link KafkaToIgniteCdcStreamer} from Srping xml file. */
public class KafkaToIgniteLoaderTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testLoadConfig() throws Exception {
        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("kafka-to-ignite-2.xml"),
            IgniteCheckedException.class,
            "Exact 1 IgniteConfiguration should be defined. Found 2"
        );

        assertThrowsWithCause(
            () -> loadKafkaToIgniteStreamer("kafka-to-ignite-3.xml"),
            IgniteCheckedException.class
        );

        KafkaToIgniteCdcStreamer streamer = loadKafkaToIgniteStreamer("kafka-to-ignite-correct.xml");

        assertNotNull(streamer);
    }
}
