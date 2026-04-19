package com.apollo.transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class LockManager {
    private final LockTable lockTable = new LockTable();
    private final DeadlockDetector deadlockDetector = new DeadlockDetector();
    private final Map<Long, Set<Long>> waitForGraph = new HashMap<>();

    public synchronized boolean acquireShared(String resource, long transactionId) {
        boolean acquired = lockTable.tryAcquireShared(resource, transactionId);
        if (acquired) {
            waitForGraph.remove(transactionId);
        }
        return acquired;
    }

    public synchronized boolean acquireExclusive(String resource, long transactionId) {
        boolean acquired = lockTable.tryAcquireExclusive(resource, transactionId);
        if (acquired) {
            waitForGraph.remove(transactionId);
        }
        return acquired;
    }

    public synchronized void registerWait(long waiter, long owner) {
        waitForGraph.put(waiter, Set.of(owner));
    }

    public synchronized Optional<Long> detectDeadlockVictim() {
        return deadlockDetector.youngestVictim(waitForGraph);
    }

    public synchronized Map<Long, Set<Long>> waitForGraph() {
        return Map.copyOf(waitForGraph);
    }

    public synchronized void release(String resource, long transactionId) {
        lockTable.release(resource, transactionId);
        waitForGraph.remove(transactionId);
    }
}
