package com.apollo.transaction;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeadlockDetectorTest {
    @Test
    void testDeadlockDetectedAndYoungestVictimChosen() {
        DeadlockDetector detector = new DeadlockDetector();
        Map<Long, Set<Long>> graph = Map.of(
                1L, Set.of(2L),
                2L, Set.of(3L),
                3L, Set.of(1L)
        );

        assertTrue(detector.hasDeadlock(graph));
        assertEquals(3L, detector.youngestVictim(graph).orElseThrow());
    }

    @Test
    void testLockManagerTracksWaitForGraph() {
        LockManager lockManager = new LockManager();
        lockManager.registerWait(10L, 11L);
        lockManager.registerWait(11L, 10L);

        assertEquals(11L, lockManager.detectDeadlockVictim().orElseThrow());
    }
}
