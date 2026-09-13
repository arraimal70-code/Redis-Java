package com.redisclone.storage.eviction;

import java.util.HashMap;
import java.util.Map;

/**
 * High-performance O(1) Least Recently Used (LRU) eviction policy.
 * Maintained via an internal doubly-linked list coupled with a hash lookup map.
 * Head represents the most recently accessed key; tail represents the least recently used key.
 */
public class LruEvictionPolicy implements EvictionPolicy {

    private static class Node {
        final String key;
        Node prev;
        Node next;

        Node(String key) {
            this.key = key;
        }
    }

    private final String name;
    private final Map<String, Node> nodeMap = new HashMap<>();
    private final Node head;
    private final Node tail;

    public LruEvictionPolicy(String name) {
        this.name = name;
        this.head = new Node(null);
        this.tail = new Node(null);
        head.next = tail;
        tail.prev = head;
    }

    public static LruEvictionPolicy allKeysLru() {
        return new LruEvictionPolicy("allkeys-lru");
    }

    @Override
    public synchronized void onKeyAccess(String key) {
        Node node = nodeMap.get(key);
        if (node != null) {
            unlink(node);
            attachHead(node);
        }
    }

    @Override
    public synchronized void onKeyInsert(String key) {
        Node existing = nodeMap.get(key);
        if (existing != null) {
            unlink(existing);
            attachHead(existing);
        } else {
            Node newNode = new Node(key);
            nodeMap.put(key, newNode);
            attachHead(newNode);
        }
    }

    @Override
    public synchronized void onKeyDelete(String key) {
        Node node = nodeMap.remove(key);
        if (node != null) {
            unlink(node);
        }
    }

    @Override
    public synchronized String evictKey() {
        if (nodeMap.isEmpty()) {
            return null;
        }
        // Least recently used is right before tail sentinel
        Node lru = tail.prev;
        if (lru == head) {
            return null;
        }
        unlink(lru);
        nodeMap.remove(lru.key);
        return lru.key;
    }

    @Override
    public String getName() {
        return name;
    }

    private void attachHead(Node node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }

    private void unlink(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
    }

    public synchronized int size() {
        return nodeMap.size();
    }
}
