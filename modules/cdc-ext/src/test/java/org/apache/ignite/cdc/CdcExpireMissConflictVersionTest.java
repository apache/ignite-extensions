package org.apache.ignite.cdc;

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.CdcEventImpl;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class CdcExpireMissConflictVersionTest extends GridCommonAbstractTest {
    /** */
    private volatile DataEntry entry;

    /** */
    private byte clusterId;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setCdcEnabled(true)));

        cfg.setCacheConfiguration(new CacheConfiguration<Long, Object>()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        CacheVersionConflictResolverPluginProvider provider = new CacheVersionConflictResolverPluginProvider();
        provider.setClusterId(clusterId);
        provider.setCaches(F.asSet(DEFAULT_CACHE_NAME));

        cfg.setPluginProviders(new WalPluginProvider(), provider);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void test() throws Exception {
        clusterId = (byte)2;

        IgniteEx ign = startGrid(0);
        ign.cluster().state(ClusterState.ACTIVE);

        BinaryObject bo = ign.binary().builder("simple")
            .setField("val", 10)
            .build();

        ExpiryPolicy plc = new ExpiryPolicy() {
            @Override public Duration getExpiryForCreation() {
                return new Duration(TimeUnit.MILLISECONDS, 10_000);
            }

            @Override public Duration getExpiryForAccess() {
                return null;
            }

            @Override public Duration getExpiryForUpdate() {
                return null;
            }
        };

        ign.cache(DEFAULT_CACHE_NAME)
            .withKeepBinary()
            .withExpiryPolicy(plc)
            .put(0L, bo);

        assertTrue(GridTestUtils.waitForCondition(() -> entry != null, 10_000, 10));

        assertEquals(clusterId, entry.writeVersion().dataCenterId());

        DataEntry e = entry;

        entry = null;

        stopGrid(0);

        clusterId = (byte)1;

        ign = startGrid(0);
        ign.cluster().state(ClusterState.ACTIVE);

        ign.cachex(DEFAULT_CACHE_NAME)
            .keepBinary()
            .withExpiryPolicy(plc)
            .putAllConflict(F.asMap(
                e.key(),
                drInfo(e.value(), e.expireTime(), e.writeVersion())
            ));

        assertTrue(GridTestUtils.waitForCondition(() -> entry != null, 10_000, 10));

        System.out.println("ENTRY = " + e + ", " + entry);

        // Version of DataEntry must contain conflict version to be filtered on CDC.
        assertTrue(entry.writeVersion() instanceof GridCacheVersionEx);
    }

    /** */
    private GridCacheDrInfo drInfo(Object val, long expireTime, CacheEntryVersion order) {
        CdcEventsIgniteApplier applier = new CdcEventsIgniteApplier(null, 0, log);

        CdcEvent ev = new CdcEventImpl(null, val, false, 0, null, 0, expireTime);

        GridCacheVersion ver = new GridCacheVersion(order.topologyVersion(), order.order() + 1, order.nodeOrder(), order.clusterId());

        return applier.toValue(0, ev, ver);
    }

    /** */
    private class WalPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "wal-plugin-provider";
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
            if (IgniteWriteAheadLogManager.class.equals(cls))
                return (T)new WalManager(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** */
    private class WalManager extends FileWriteAheadLogManager {
        /** */
        public WalManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public WALPointer log(WALRecord rec) throws IgniteCheckedException {
            if (rec.type() == WALRecord.RecordType.DATA_RECORD_V2)
                entry = ((DataRecord)rec).writeEntries().get(0);

            return super.log(rec);
        }
    }
}
