package com.apollo.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

public class BPlusTree<K extends Comparable<K>, V> {
    private final int order;
    private final TreeMap<K, V> values = new TreeMap<>();
    private BTreeNode<K, V> root = new BTreeNode<>(true);

    public BPlusTree(int order) {
        if (order < 3) {
            throw new IllegalArgumentException("order must be >= 3");
        }
        this.order = order;
    }

    public synchronized void insert(K key, V value) {
        Objects.requireNonNull(key, "key");
        values.put(key, value);
        rebuild();
    }

    public synchronized Optional<V> search(K key) {
        return Optional.ofNullable(values.get(key));
    }

    public synchronized boolean delete(K key) {
        V removed = values.remove(key);
        if (removed != null) {
            rebuild();
            return true;
        }
        return false;
    }

    public synchronized List<V> rangeScan(K startInclusive, K endInclusive) {
        return new ArrayList<>(values.subMap(startInclusive, true, endInclusive, true).values());
    }

    public synchronized BTreeIterator<K, V> iterator(K startInclusive) {
        return new BTreeIterator<>(values.tailMap(startInclusive, true));
    }

    public synchronized int height() {
        int height = 0;
        BTreeNode<K, V> node = root;
        while (node != null) {
            height++;
            if (node.leaf || node.children.isEmpty()) {
                break;
            }
            node = castNode(node.children.get(0));
        }
        return height;
    }

    public synchronized boolean isBalanced() {
        List<Integer> depths = new ArrayList<>();
        collectLeafDepths(root, 1, depths);
        return depths.stream().distinct().count() <= 1;
    }

    public synchronized int size() {
        return values.size();
    }

    public synchronized void clear() {
        values.clear();
        root = new BTreeNode<>(true);
    }

    public synchronized void putAll(Map<K, V> entries) {
        values.clear();
        values.putAll(entries);
        rebuild();
    }

    private void rebuild() {
        List<BTreeNode<K, V>> leaves = new ArrayList<>();
        BTreeNode<K, V> currentLeaf = null;
        int count = 0;
        for (NavigableMap.Entry<K, V> entry : values.entrySet()) {
            if (currentLeaf == null || count == order - 1) {
                BTreeNode<K, V> nextLeaf = new BTreeNode<>(true);
                if (currentLeaf != null) {
                    currentLeaf.next = nextLeaf;
                }
                currentLeaf = nextLeaf;
                leaves.add(currentLeaf);
                count = 0;
            }
            currentLeaf.keys.add(entry.getKey());
            currentLeaf.children.add(entry.getValue());
            count++;
        }

        if (leaves.isEmpty()) {
            root = new BTreeNode<>(true);
            return;
        }

        List<BTreeNode<K, V>> level = leaves;
        while (level.size() > 1) {
            List<BTreeNode<K, V>> parents = new ArrayList<>();
            for (int i = 0; i < level.size(); i += order) {
                int end = Math.min(i + order, level.size());
                List<BTreeNode<K, V>> group = level.subList(i, end);
                BTreeNode<K, V> parent = new BTreeNode<>(false);
                parent.children.addAll(group);
                for (int j = 1; j < group.size(); j++) {
                    parent.keys.add(firstKey(group.get(j)));
                }
                parents.add(parent);
            }
            level = parents;
        }

        root = level.get(0);
    }

    private void collectLeafDepths(BTreeNode<K, V> node, int depth, List<Integer> depths) {
        if (node.leaf) {
            depths.add(depth);
            return;
        }
        for (Object child : node.children) {
            collectLeafDepths(castNode(child), depth + 1, depths);
        }
    }

    @SuppressWarnings("unchecked")
    private BTreeNode<K, V> castNode(Object node) {
        return (BTreeNode<K, V>) node;
    }

    private K firstKey(BTreeNode<K, V> node) {
        if (node.leaf) {
            return node.keys.get(0);
        }
        return firstKey(castNode(node.children.get(0)));
    }
}
