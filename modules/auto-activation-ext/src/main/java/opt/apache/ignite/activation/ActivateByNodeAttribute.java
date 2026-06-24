package opt.apache.ignite.activation;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Activate cluster when nodes with all specified attributes values join topology.
 */
public class ActivateByNodeAttribute implements IgnitePredicate<Collection<ClusterNode>> {
    /** Node's attribute name. */
    private final String attrName;

    /** Collection of values for node's attribute. */
    private final Set<String> requiredValues;

    /**
     * @param attributeName Node's attribute name.
     * @param requiredValues List of values for node's attribute.
     */
    public ActivateByNodeAttribute(String attributeName, Set<String> requiredValues) {
        if (attributeName == null || attributeName.isBlank())
            throw new IllegalArgumentException("attributeName must be set");

        if (requiredValues == null || requiredValues.isEmpty())
            throw new IllegalArgumentException("requiredValues must be set");

        this.attrName = attributeName;
        this.requiredValues = requiredValues;
    }

    /** */
    @Override public boolean apply(Collection<ClusterNode> nodes) {
        Set<String> missingNodes = new LinkedHashSet<String>(requiredValues);

        for (ClusterNode node : nodes) {
            String attrVal = node.attribute(attrName);

            missingNodes.remove(attrVal);

            if (missingNodes.isEmpty()) break;
        }

        return missingNodes.isEmpty();
    }
}
