package com.apollo.storage;

import java.util.ArrayList;
import java.util.List;

class BTreeNode<K extends Comparable<K>, V> {
    boolean leaf;
    List<K> keys = new ArrayList<>();
    List<Object> children = new ArrayList<>();
    BTreeNode<K, V> next;

    BTreeNode(boolean leaf) {
        this.leaf = leaf;
    }
}
