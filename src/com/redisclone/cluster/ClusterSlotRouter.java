package com.redisclone.cluster;

import com.redisclone.resp.RespFrame;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages Redis Cluster 16,384 slot sharding, ownership mapping, and -MOVED redirection.
 */
public class ClusterSlotRouter {

    public record ClusterNode(String id, String host, int port, int startSlot, int endSlot) {}

    private boolean clusterEnabled = false;
    private final String selfHost;
    private final int selfPort;
    private int myStartSlot = 0;
    private int myEndSlot = 16383;
    private final List<ClusterNode> clusterTopology = new ArrayList<>();

    public ClusterSlotRouter(String selfHost, int selfPort) {
        this.selfHost = selfHost;
        this.selfPort = selfPort;
        // Default standalone ownership: all 16384 slots
        this.clusterTopology.add(new ClusterNode("node-0", selfHost, selfPort, 0, 16383));
    }

    public boolean isClusterEnabled() {
        return clusterEnabled;
    }

    public void setClusterEnabled(boolean enabled) {
        this.clusterEnabled = enabled;
    }

    public void setMySlotRange(int start, int end) {
        this.myStartSlot = start;
        this.myEndSlot = end;
        if (!clusterTopology.isEmpty()) {
            clusterTopology.set(0, new ClusterNode("node-0", selfHost, selfPort, start, end));
        }
    }

    public void addRemoteNode(String id, String host, int port, int startSlot, int endSlot) {
        clusterTopology.add(new ClusterNode(id, host, port, startSlot, endSlot));
    }

    public boolean isMySlot(int slot) {
        if (!clusterEnabled) return true;
        return slot >= myStartSlot && slot <= myEndSlot;
    }

    public ClusterNode getNodeForSlot(int slot) {
        for (ClusterNode node : clusterTopology) {
            if (slot >= node.startSlot() && slot <= node.endSlot()) {
                return node;
            }
        }
        return new ClusterNode("unknown", selfHost, selfPort, 0, 16383);
    }

    /**
     * Generates a Redis Cluster protocol -MOVED redirection error frame.
     */
    public RespFrame createMovedError(int slot) {
        ClusterNode owner = getNodeForSlot(slot);
        return RespFrame.ofError("MOVED " + slot + " " + owner.host() + ":" + owner.port());
    }

    public List<ClusterNode> getClusterTopology() {
        return List.copyOf(clusterTopology);
    }
}
