package com.apollo.storage;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BPlusTreeTest {
    @Test
    void testBtreeInsertSearch() {
        BPlusTree<Integer, Integer> tree = new BPlusTree<>(8);
        for (int i = 0; i < 10_000; i++) {
            tree.insert(i, i * 10);
        }

        for (int i = 0; i < 10_000; i++) {
            assertEquals(i * 10, tree.search(i).orElseThrow());
        }
    }

    @Test
    void testBtreeRangeScan() {
        BPlusTree<Integer, Integer> tree = new BPlusTree<>(4);
        for (int i = 0; i < 100; i++) {
            tree.insert(i, i);
        }

        List<Integer> values = tree.rangeScan(10, 19);
        assertEquals(10, values.size());
        assertEquals(10, values.get(0));
        assertEquals(19, values.get(9));
    }

    @Test
    void testBtreeSplit() {
        BPlusTree<Integer, Integer> tree = new BPlusTree<>(4);
        for (int i = 0; i < 1_000; i++) {
            tree.insert(i, i);
        }

        assertTrue(tree.isBalanced());
        assertTrue(tree.height() > 1);
    }
}
