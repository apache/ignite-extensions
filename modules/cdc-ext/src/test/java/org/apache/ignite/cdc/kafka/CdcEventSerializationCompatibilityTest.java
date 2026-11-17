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

package org.apache.ignite.cdc.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.CdcEventImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cdc.kafka.CdcEventInputStream.INCOMPATIBLE_CLS;
import static org.apache.ignite.compatibility.IgniteReleasedVersion.VER_2_17_0;
import static org.apache.ignite.compatibility.IgniteReleasedVersion.VER_2_18_0;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Checks that a {@link CdcEvent} serialized by one Ignite version can be deserialized by another one.
 * <p>
 * Kafka based CDC replication writes events to a topic with plain Java serialization
 * (see {@link IgniteToKafkaCdcStreamer}) and reads them with {@link ObjectInputStream#readObject()}
 * (see {@link KafkaToIgniteCdcStreamerApplier}). The source and the destination clusters, and so
 * {@code ignite-cdc.sh} and {@code kafka-to-ignite.sh}, can be of different Ignite versions.
 * <p>
 * The test emulates this flow without Kafka: events are created and serialized by a node of one version,
 * then read and compared with the expected events by a node of another version.
 * Nodes of different versions are started one after another, data between JVMs are passed without files:
 * with the thin client from the old version node and with the node start closure to the old version node.
 * <p>
 * Events of an incompatible version can't be read with the default {@link ObjectInputStream},
 * {@link CdcEventInputStream} must be used instead.
 */
@RunWith(Parameterized.class)
public class CdcEventSerializationCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** Prefix of the names of the events that contain binary objects. */
    private static final String INCOMPATIBLE_DATA = "binaryObject";

    /** Old Ignite version. */
    @Parameterized.Parameter
    public IgniteReleasedVersion oldVer;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "oldVer={0}")
    public static Collection<IgniteReleasedVersion> versions() {
        return List.of(VER_2_17_0, VER_2_18_0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Events written by old version must be readable by the current one with the {@link CdcEventInputStream}.
     * {@link ObjectInputStream} fails for incompatible versions.
     */
    @Test
    public void testOldEventRead() throws Exception {
        startGrid(
            1,
            oldVer.toString(),
            cfg -> {/* No-op. */},
            ign -> ign.getOrCreateCache(DEFAULT_CACHE_NAME).putAll(serializedEvents(ign))
        );

        Map<String, byte[]> oldNodeEvts = new LinkedHashMap<>();

        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
            ClientCache<String, byte[]> cache = cli.cache(DEFAULT_CACHE_NAME);

            for (Cache.Entry<String, byte[]> e : cache.query(new ScanQuery<String, byte[]>()).getAll())
                oldNodeEvts.put(e.getKey(), e.getValue());
        }

        stopAllGrids();

        IgniteEx ign = startGrid(0);

        deserializeAndCheck(ign, oldNodeEvts);
    }

    /**
     * Events written by current version must be readable by the old one with the {@link CdcEventInputStream}.
     * Default {@link ObjectInputStream} fails for incompatible versions.
     */
    @Test
    public void testCurrentEventReadByOld() throws Exception {
        Map<String, byte[]> curNodeEvts = serializedEvents(startGrid(0));

        stopAllGrids();

        // Serialized events are passed to the old version node inside the closure.
        startGrid(
            1,
            oldVer.toString(),
            cfg -> {/* No-op. */},
            ign -> deserializeAndCheck((IgniteEx)ign, curNodeEvts)
        );
    }

    /** */
    private void deserializeAndCheck(IgniteEx ign, Map<String, byte[]> serializedEvts) {
        String oldName = U.setCurrentIgniteName(ign.name());

        try {
            for (Map.Entry<String, CdcEvent> e : events(ign).entrySet()) {
                String name = e.getKey();
                byte[] oldNodeEvt = serializedEvts.get(name);

                assertNotNull(name, oldNodeEvt);
                assertTrue(name, same(e.getValue(), read(new CdcEventInputStream(new ByteArrayInputStream(oldNodeEvt)))));

                if (incompatible(name)) {
                    assertThrows(
                        log,
                        () -> read(new ObjectInputStream(new ByteArrayInputStream(oldNodeEvt))),
                        InvalidClassException.class,
                        INCOMPATIBLE_CLS
                    );
                }
                else
                    assertTrue(name, same(e.getValue(), read(new ObjectInputStream(new ByteArrayInputStream(oldNodeEvt)))));
            }
        }
        catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            U.restoreOldIgniteName(oldName, ign.name());
        }
    }

    /**
     * @param evtName Event name.
     * @return {@code True} if the event can't be read by the current version (and vice versa) with the default {@link ObjectInputStream}.
     */
    private boolean incompatible(String evtName) {
        return evtName.startsWith(INCOMPATIBLE_DATA) && VER_2_17_0.equals(oldVer);
    }

    /** */
    private static Map<String, CdcEvent> events(Ignite ign) {
        GridCacheVersion ver = new GridCacheVersion(1, 2L, 3, 4);
        GridCacheVersion conflictVer = new GridCacheVersionEx(1, 3, 5L, new GridCacheVersion(1, 2L, 3, 5));

        BinaryObject key = ign.binary().builder("CdcTestKey")
            .setField("id", 1)
            .build();

        BinaryObject val = ign.binary().builder("CdcTestValue")
            .setField("name", "Sarah Connor")
            .setField("year", 1984L)
            .build();

        Map<String, CdcEvent> evts = new LinkedHashMap<>();

        evts.put("1", new CdcEventImpl(1, "val", true, 1, ver, CU.cacheId(DEFAULT_CACHE_NAME), 0));
        evts.put("2", new CdcEventImpl(2, "val", false, 2, conflictVer, CU.cacheId(DEFAULT_CACHE_NAME), 0));
        evts.put(INCOMPATIBLE_DATA + "3", new CdcEventImpl(key, val, true, 3, ver, CU.cacheId(DEFAULT_CACHE_NAME), 100));
        evts.put(INCOMPATIBLE_DATA + "4", new CdcEventImpl(key, null, true, 3, ver, CU.cacheId(DEFAULT_CACHE_NAME), 0));

        return evts;
    }

    /** Serializes events the same way {@link IgniteToKafkaCdcStreamer} does. */
    private static Map<String, byte[]> serializedEvents(Ignite ign) {
        Map<String, byte[]> res = new LinkedHashMap<>();

        try {
            for (Map.Entry<String, CdcEvent> e : events(ign).entrySet()) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();

                // The same as IgniteUtils#toBytes(Serializable).
                try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                    oos.writeObject(e.getValue());
                }

                res.put(e.getKey(), bos.toByteArray());
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return res;
    }

    /** */
    private static CdcEvent read(ObjectInputStream is) throws IOException, ClassNotFoundException {
        try (ObjectInputStream is0 = is) {
            return (CdcEvent)is0.readObject();
        }
    }

    /** */
    private static boolean same(CdcEvent exp, CdcEvent act) {
        return Objects.equals(exp.key(), act.key())
            && Objects.equals(exp.value(), act.value())
            && exp.primary() == act.primary()
            && exp.partition() == act.partition()
            && Objects.equals(exp.version(), act.version())
            && Objects.equals(exp.version().otherClusterVersion(), act.version().otherClusterVersion())
            && exp.cacheId() == act.cacheId()
            && exp.expireTime() == act.expireTime();
    }
}
