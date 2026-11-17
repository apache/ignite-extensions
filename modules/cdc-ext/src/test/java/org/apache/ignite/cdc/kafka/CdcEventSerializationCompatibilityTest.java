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
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.CdcEventImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
    /** Thin client address of the old version node. */
    private static final String ADDR = "127.0.0.1:10800";

    /** Name of the cache on the old version node to pass data to the test JVM. */
    private static final String EXCHANGE_CACHE = "exchange";

    /** Suffix of the {@link #EXCHANGE_CACHE} key for the result of read with the default {@link ObjectInputStream}. */
    private static final String DFLT_STREAM = ":ObjectInputStream";

    /** Suffix of the {@link #EXCHANGE_CACHE} key for the result of read with the {@link CdcEventInputStream}. */
    private static final String CDC_STREAM = ":CdcEventInputStream";

    /** Prefix of the names of the events that contain binary objects. */
    private static final String BINARY_EVT_PREFIX = "binaryObject";

    /**
     * Class that had no explicit {@code serialVersionUID} before the fix.
     * The value computed by JVM in 2.17 and earlier versions is the explicit value now.
     * The value computed in 2.18.0 differs.
     */
    private static final String INCOMPATIBLE_CLS = "org.apache.ignite.internal.binary.BinaryObjectExImpl";

    /** Versions with the {@code serialVersionUID} of the {@link #INCOMPATIBLE_CLS} that differs from the current one. */
    private static final Set<String> INCOMPATIBLE_VERS = Set.of("2.18.0");

    /** Cache id. */
    private static final int CACHE_ID = 42;

    /** Old Ignite version. */
    @Parameterized.Parameter
    public String oldVer;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "oldVer={0}")
    public static Collection<String> versions() {
        return List.of("2.17.0", "2.18.0");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Events written by old version must be readable by the current one with the {@link CdcEventInputStream}.
     * Default {@link ObjectInputStream} fails for incompatible versions.
     */
    @Test
    public void testOldVersionEventReadByCurrent() throws Exception {
        startGrid(1, oldVer, cfg -> {
            // No-op.
        }, ign -> ign.getOrCreateCache(EXCHANGE_CACHE).putAll(serializedEvents(ign)));

        Map<String, byte[]> serialized = exchangeCacheContent();

        stopAllGrids();

        IgniteEx ign = startGrid(0);

        // The same as KafkaToIgniteCdcStreamer does: binary objects are bound to the node on deserialization.
        String oldName = U.setCurrentIgniteName(ign.name());

        try {
            for (Map.Entry<String, CdcEvent> e : events(ign).entrySet()) {
                String name = e.getKey();
                CdcEvent exp = e.getValue();

                byte[] bytes = serialized.get(name);

                assertNotNull(name, bytes);

                if (incompatible(name)) {
                    assertThrows(log, () -> read(new ObjectInputStream(new ByteArrayInputStream(bytes))),
                        InvalidClassException.class, INCOMPATIBLE_CLS);
                }
                else {
                    assertTrue(name + ": unexpected event read by ObjectInputStream",
                        same(exp, read(new ObjectInputStream(new ByteArrayInputStream(bytes)))));
                }

                assertTrue(name + ": unexpected event read by CdcEventInputStream",
                    same(exp, read(new CdcEventInputStream(new ByteArrayInputStream(bytes)))));
            }
        }
        finally {
            U.restoreOldIgniteName(oldName, ign.name());
        }
    }

    /**
     * Events written by current version must be readable by the old one with the {@link CdcEventInputStream}.
     * Default {@link ObjectInputStream} fails for incompatible versions.
     */
    @Test
    public void testCurrentVersionEventReadByOld() throws Exception {
        Map<String, byte[]> serialized = serializedEvents(startGrid(0));

        stopAllGrids();

        // Serialized events are passed to the old version node inside the closure.
        startGrid(1, oldVer, cfg -> {
            // No-op.
        }, ign -> ign.getOrCreateCache(EXCHANGE_CACHE).putAll(readOnOldVersion(ign, serialized)));

        Map<String, String> res = exchangeCacheContent();

        for (String name : serialized.keySet()) {
            String dfltErr = res.get(name + DFLT_STREAM);
            String cdcErr = res.get(name + CDC_STREAM);

            assertNotNull(name, dfltErr);
            assertNotNull(name, cdcErr);

            if (incompatible(name)) {
                assertTrue(name + ": " + dfltErr,
                    dfltErr.contains(InvalidClassException.class.getName()) && dfltErr.contains(INCOMPATIBLE_CLS));
            }
            else
                assertTrue(name + ": " + dfltErr, dfltErr.isEmpty());

            assertTrue(name + ": " + cdcErr, cdcErr.isEmpty());
        }
    }

    /**
     * @param evtName Event name.
     * @return {@code True} if the event written by {@link #oldVer} can't be read by the current version
     *      (and vice versa) with the default {@link ObjectInputStream}.
     */
    private boolean incompatible(String evtName) {
        return evtName.startsWith(BINARY_EVT_PREFIX) && INCOMPATIBLE_VERS.contains(oldVer);
    }

    /**
     * @return Content of the {@link #EXCHANGE_CACHE} of the old version node.
     * @param <V> Type of the cache value.
     */
    private static <V> Map<String, V> exchangeCacheContent() throws Exception {
        Map<String, V> res = new LinkedHashMap<>();

        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(ADDR))) {
            for (Cache.Entry<String, V> e : cli.<String, V>cache(EXCHANGE_CACHE).query(new ScanQuery<String, V>()).getAll())
                res.put(e.getKey(), e.getValue());
        }

        return res;
    }

    /**
     * Note, the method is executed both by the current and by the old Ignite version,
     * so only the API available in all tested versions can be used.
     *
     * @param ign Node to build binary objects with.
     * @return Events to check, by name.
     */
    private static Map<String, CdcEvent> events(Ignite ign) {
        GridCacheVersion ver = new GridCacheVersion(1, 2L, 3, 4);
        GridCacheVersion conflictVer = new GridCacheVersionEx(1, 3, 5L, new GridCacheVersion(1, 2L, 3, 5));

        BinaryObject key = ign.binary().builder("CdcTestKey")
            .setField("id", 1)
            .build();

        BinaryObject val = ign.binary().builder("CdcTestValue")
            .setField("name", "val")
            .setField("amount", 42L)
            .build();

        Map<String, CdcEvent> evts = new LinkedHashMap<>();

        evts.put("primitive", new CdcEventImpl(1, "val", true, 1, ver, CACHE_ID, 0));
        evts.put("primitiveConflictVersion", new CdcEventImpl(2, "val", false, 2, conflictVer, CACHE_ID, 0));
        evts.put(BINARY_EVT_PREFIX, new CdcEventImpl(key, val, true, 3, ver, CACHE_ID, 100));
        evts.put(BINARY_EVT_PREFIX + "Remove", new CdcEventImpl(key, null, true, 3, ver, CACHE_ID, 0));

        return evts;
    }

    /**
     * Serializes events the same way {@link IgniteToKafkaCdcStreamer} does.
     * Note, the method is executed both by the current and by the old Ignite version.
     *
     * @param ign Node.
     * @return Serialized events, by name.
     */
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

    /**
     * Reads events with the default {@link ObjectInputStream} and with the {@link CdcEventInputStream}
     * on the old version node and compares them with expected.
     *
     * @param ign Node.
     * @param serialized Serialized events, by name.
     * @return Error description (empty if event successfully read), by event name with the stream suffix.
     */
    private static Map<String, String> readOnOldVersion(Ignite ign, Map<String, byte[]> serialized) {
        Map<String, String> res = new LinkedHashMap<>();

        String oldName = U.setCurrentIgniteName(ign.name());

        try {
            for (Map.Entry<String, CdcEvent> e : events(ign).entrySet()) {
                for (String stream : List.of(DFLT_STREAM, CDC_STREAM)) {
                    String err;

                    try {
                        InputStream in = new ByteArrayInputStream(serialized.get(e.getKey()));

                        CdcEvent act = read(DFLT_STREAM.equals(stream) ? new ObjectInputStream(in) : new CdcEventInputStream(in));

                        err = same(e.getValue(), act) ? "" : "unexpected event [exp=" + e.getValue() + ", act=" + act + ']';
                    }
                    catch (Throwable t) {
                        err = String.valueOf(t);
                    }

                    res.put(e.getKey() + stream, err);
                }
            }
        }
        finally {
            U.restoreOldIgniteName(oldName, ign.name());
        }

        return res;
    }

    /**
     * @param is Stream to read event from.
     * @return Event.
     */
    private static CdcEvent read(ObjectInputStream is) throws IOException, ClassNotFoundException {
        try (ObjectInputStream is0 = is) {
            return (CdcEvent)is0.readObject();
        }
    }

    /**
     * @param exp Expected event.
     * @param act Actual event.
     * @return {@code True} if events are the same.
     */
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
