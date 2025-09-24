package org.apache.ignite.plugin.mdc;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.CacheTopologyValidatorProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

/** */
public class SegmentationProtectionPluginProvider implements PluginProvider<PluginConfiguration> {
    /** */
    private static final String MDC_SEGMENTATION_PROTECTION_PLUGIN_PROVIDER_NAME = "MdcSegmentationProtectionPluginProvider";

    /** */
    private static final String MDC_SEGMENTATION_PROTECTION_PLUGIN_VERSION = "1.0.0";

    /** */
    private boolean topValidatorEnabled = true;

    /** */
    private volatile TopologyValidatorImpl topValidator;

    /** */
    public SegmentationProtectionPluginProvider() {

    }

    /** */
    public SegmentationProtectionPluginProvider(boolean topValidatorEnabled) {
        this.topValidatorEnabled = topValidatorEnabled;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return MDC_SEGMENTATION_PROTECTION_PLUGIN_PROVIDER_NAME;
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return MDC_SEGMENTATION_PROTECTION_PLUGIN_VERSION;
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin() {
        return (T)new IgnitePlugin() {
            // No-op.
        };
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
        GridKernalContext kCtx = ((IgniteEx)ctx.grid()).context();

        topValidator = new TopologyValidatorImpl();

        if (!kCtx.clientNode())
            registry.registerExtension(CacheTopologyValidatorProvider.class, cacheName -> topValidator);
    }

    /** {@inheritDoc} */
    @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) {
        MultiDcTopologyState topState = new MultiDcTopologyState(ctx.log(MultiDcTopologyState.class));

        topState.init(((IgniteEx)ctx.grid()).context());

        topValidator.setTopologyState(topState);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public @Nullable Serializable provideDiscoveryData(UUID nodeId) {
        // TODO implement configuration validation: all nodes should have the same MAIN_DC
        return null;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // TODO implement configuration validation: all nodes should have the same MAIN_DC
    }
}
