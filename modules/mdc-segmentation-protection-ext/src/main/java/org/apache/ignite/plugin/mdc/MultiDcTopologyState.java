package org.apache.ignite.plugin.mdc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryLocalJoinData;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DATA_CENTER_ID;

/** */
public class MultiDcTopologyState implements DiscoveryEventListener {
    /** */
    private static final String LEADER_DC_KEY = "leaderDc";

    /** */
    private final AtomicBoolean awaitingJoin = new AtomicBoolean(true);

    /** */
    private final Map<String, Integer> topMap = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /** */
    private volatile String leaderDcId;

    /** */
    MultiDcTopologyState(IgniteLogger log) {
        this.log = log;
    }

    /** */
    void init(GridKernalContext ctx) {
        ctx.event().addDiscoveryEventListener(
            this,
            EVT_NODE_JOINED,
            EVT_NODE_FAILED,
            EVT_NODE_LEFT
        );

        ctx.internalSubscriptionProcessor().registerDistributedMetastorageListener(
            new DistributedMetastorageLifecycleListener() {
                @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                    ctx.closure().runLocalSafe(() -> {
                        String locNodeDcId = (String)ctx.nodeAttribute(ATTR_DATA_CENTER_ID);

                        try {
                            boolean leaderDcInitialized = metastorage.compareAndSet(LEADER_DC_KEY, null, locNodeDcId);

                            if (leaderDcInitialized)
                                setLeaderDcId(locNodeDcId);
                            else {
                                String leaderDc = metastorage.read(LEADER_DC_KEY);
                                setLeaderDcId(leaderDc);
                            }
                        }
                        catch (IgniteCheckedException e) {
                            log.error("Failed to set leader DC", e);
                        }
                    }, true);
                }
            }
        );

        ctx.distributedMetastorage().listen(
            (key) -> key.equals(LEADER_DC_KEY),
            (key, oldVal, newVal) -> {
                if (oldVal == null || oldVal.equals(newVal))
                    return;

                setLeaderDcId((String)newVal);
            });

        ctx.discovery().localJoinFuture().listen(this::onLocalJoinEvent);
    }

    /**
     * @param fut Future object with local join result.
     */
    private void onLocalJoinEvent(IgniteInternalFuture<DiscoveryLocalJoinData> fut) {
        if (fut.error() == null) {
            awaitingJoin.set(false);

            List<ClusterNode> allNodes = null;

            try {
                allNodes = fut.get().discoCache().allNodes();
            } catch (IgniteCheckedException e) {
                // No-op for now.
            }

            if (allNodes != null) {
                Collection<String> allDcIds = allNodes.stream()
                    .filter(n -> !n.isClient())
                    .map(n -> (String)n.attribute(ATTR_DATA_CENTER_ID))
                    .collect(Collectors.toCollection(ArrayList<String>::new));

                initTopology(allDcIds);
            }
        }
    }

    /** */
    boolean isTopologyConnected() {
        if (awaitingJoin.get())
            return true;

        return topMap.get(leaderDcId) != null && topMap.get(leaderDcId) != 0;
    }

    /** */
    void setLeaderDcId(String leaderDcId) {
        this.leaderDcId = leaderDcId;
    }

    /** {@inheritDoc} */
    @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
        if (evt.eventNode().isClient())
            return;

        String nodeDcId = evt.eventNode().attribute(ATTR_DATA_CENTER_ID);

        switch (evt.type()) {
            case EVT_NODE_JOINED:
                onNodeJoined(nodeDcId);
                break;
            case EVT_NODE_FAILED:
            case EVT_NODE_LEFT:
                onNodeLeft(nodeDcId);
        }
    }

    /**
     * @param nodesDcs Nodes.
     */
    void initTopology(Collection<String> nodesDcs) {
        nodesDcs.forEach(nodeDc -> topMap.compute(nodeDc, (k, v) -> (v == null ? 1 : v + 1)));
    }

    /** */
    private void onNodeJoined(String nodeDcId) {
        topMap.compute(nodeDcId, (k, v) -> (v == null ? 1 : v + 1));
    }

    /** */
    private void onNodeLeft(String nodeDcId) {
        topMap.compute(nodeDcId, (k, v) -> {
            if (v == null)
                return 0;
            else
                return (v > 0 ? v - 1 : 0);
        });
    }
}
