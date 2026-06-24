package opt.apache.ignite.activation;

import java.io.Serializable;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;

/**
 * Activate cluster when specified condition meet
 */
public class AutoActivationPluginProvider implements PluginProvider<PluginConfiguration> {
    /** */
    private final IgnitePredicate<Collection<ClusterNode>> condition;

    /** */
    private IgniteLogger logger;

    /** */
    private Ignite grid;

    /**
     * @param condition Auto activation condition.
     */
    public AutoActivationPluginProvider(IgnitePredicate<Collection<ClusterNode>> condition) {
        if (condition == null)
            throw new IllegalArgumentException("Auto activation condition must be set");

        this.condition = condition;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "Auto Activation Plugin";
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin() {
        return (T)new IgnitePlugin() {
            // No-op.
        };
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "1.0";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext pc, ExtensionRegistry er) {
        logger = pc.log(this.getClass());        
        grid = pc.grid();
    }

    /** {@inheritDoc} */
    @Override public <T> T createComponent(PluginContext pc, Class<T> type) {
        return null;
    }
 
    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext cpc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext pc) {
        // do nothing
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean bln) {
        // do nothing
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() {

        IgniteCluster cluster = grid.cluster();

        if (cluster.state() == ClusterState.ACTIVE) {
            if (logger.isInfoEnabled()) logger.info("Auto activation skipped - cluster already activated");
            return;
        }

        if (cluster.currentBaselineTopology() != null) {
            if (logger.isInfoEnabled()) logger.info("Auto activation skipped - baseline is not empty");
            return;
        }

        if (condition.apply(cluster.nodes())) {
            if (logger.isInfoEnabled()) logger.info("Auto activation plugin set cluster state ACTIVE - activation condition meet");
            cluster.state(ClusterState.ACTIVE);
        }
        else {
            if (logger.isInfoEnabled()) logger.info("Auto activation skipped - activation condition not meet");
        }
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean bln) {
        // do nothing
    }

    /** {@inheritDoc} */
    @Override public Serializable provideDiscoveryData(UUID uuid) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID uuid, Serializable srlzbl) {
        // do nothing
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode cn) throws PluginValidationException {
        // do nothing
    }
}
