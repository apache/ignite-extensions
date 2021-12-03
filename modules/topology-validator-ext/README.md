#What problem this module is intended to solve?

Some network issues can cause the Ignite cluster to split into several isolated parts - segments. Nodes from different 
segments cannot communicate with each other, while nodes from the same segment do not experience communication problems. 
In this case, each segment marks the nodes with which the connection was lost as failed and considers itself as an 
independent Ignite cluster. Let's call this scenario cluster segmentation.

Cluster segmentation can lead to cache data inconsistency across different segments because each segment can continue 
to handle cache update requests independently.

Apache Ignite allows the user to provide custom validation logic during cache configuration that will be applied to 
each topology change, and if the validation fails, writes to the corresponding cache will be blocked. The mentioned 
validation logic can be passed to Ignite cache configuration as an Ignite TopologyValidation interface implementation.

This module represents an implementation of the Ignite TopologyValidator interface which provides the guarantee that 
after cluster segmentation, no more than one segment can process write requests to the caches.

The current implementation of TopologyValidation uses remaining Ignite baseline nodes in the topology to determine 
segmentation.

#In what cases cache writes will be blocked for the segment?

The following rules are used to determine which segment can process cache write requests after segmentation and which 
cannot:

1. The segment is allowed to process cache writes requests after segmentation if and only if more than half of the 
baseline nodes remain in the segment, otherwise all writes to the cache will be blocked.
2. If the cluster is split into two equal segments, writing to both of them will be blocked. 
3. Since Ignite treats segmentation as sequential node failures, even a single node failure in a cluster in which only 
half of the baseline nodes are alive is considered as segmentation and results in write blocks for all caches.

#Configuration

1. Configure SegmentationResolverPluginProvider on each server node:

   ```
   new IgniteConfiguration() 
       ... 
       .setPluginProviders(new SegmentationResolverPluginProvider());
   ```

2. Configure IgniteCacheTopologyValidator for each cache:

   ```
   new CacheConfiguration()
       ...
       .setTopologyValidator(new IgniteCacheTopologyValidator());
   ```

3. Configure baseline nodes explicitly, or configure baseline nodes auto adjustment with a timeout that significantly 
exceeds the node failure detection timeout. It can be done through Java Api or through control script. 
See [1] and [2] for more info.

Note that it is illegal to use baseline nodes auto adjustment with a zero timeout along with current 
TopologyValidator implementation.

#Manual Segmentation resolving

The state of each segment for which cache writes were blocked will be eventually switched to the READ-ONLY mode. 
Manually switching the cluster state back to ACTIVE mode will restore cache write availability. It can be done through 
Java Api or through control script. See [1] and [2] for more info. 

[1] - https://ignite.apache.org/docs/latest/clustering/baseline-topology \
[2] - https://ignite.apache.org/docs/latest/tools/control-script#activation-deactivation-and-topology-management