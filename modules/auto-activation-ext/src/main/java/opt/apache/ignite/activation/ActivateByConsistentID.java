package opt.apache.ignite.activation;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Activate cluster when nodes with specified ConsistentID values join topology.
 */
public class ActivateByConsistentID implements IgnitePredicate<Collection<ClusterNode>> {
    /** Collection of required nodes ConsistentIDs. */
    private final Set<String> requiredNodes;

    /**
     * @param requiredNodes List of ConsistentIDs.
     */
    public ActivateByConsistentID(Set<String> requiredNodes) {
        if (requiredNodes == null || requiredNodes.isEmpty())
            throw new IllegalArgumentException("requiredNodes must be set");

        this.requiredNodes = requiredNodes;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(Collection<ClusterNode> nodes) {
        Set<String> missingNodes = new LinkedHashSet<>(requiredNodes);

        for (ClusterNode node : nodes) {
            String nodeConsistentId = node.consistentId().toString();

            missingNodes.remove(nodeConsistentId);

            if (missingNodes.isEmpty()) break;
        }

        return missingNodes.isEmpty();
    }
}
