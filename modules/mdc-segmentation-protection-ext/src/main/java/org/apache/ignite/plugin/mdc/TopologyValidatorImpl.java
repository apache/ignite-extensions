package org.apache.ignite.plugin.mdc;

import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;

/** */
public class TopologyValidatorImpl implements TopologyValidator {
    /** */
    private volatile MultiDcTopologyState topState;

    /** {@inheritDoc} */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        return topState.isTopologyConnected();
    }

    /** */
    void setTopologyState(MultiDcTopologyState topState) {
        this.topState = topState;
    }
}
