package com.apollo.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LockTable {
    public enum LockType {
        SHARED,
        EXCLUSIVE
    }

    private final Map<String, Set<Long>> sharedLocks = new HashMap<>();
    private final Map<String, Long> exclusiveLocks = new HashMap<>();

    public synchronized boolean tryAcquireShared(String resource, long transactionId) {
        Long owner = exclusiveLocks.get(resource);
        if (owner != null && owner != transactionId) {
            return false;
        }
        sharedLocks.computeIfAbsent(resource, ignored -> new HashSet<>()).add(transactionId);
        return true;
    }

    public synchronized boolean tryAcquireExclusive(String resource, long transactionId) {
        Set<Long> sharedOwners = sharedLocks.getOrDefault(resource, Set.of());
        Long exclusiveOwner = exclusiveLocks.get(resource);
        boolean conflictingShared = sharedOwners.stream().anyMatch(owner -> owner != transactionId);
        boolean conflictingExclusive = exclusiveOwner != null && exclusiveOwner != transactionId;
        if (conflictingShared || conflictingExclusive) {
            return false;
        }
        exclusiveLocks.put(resource, transactionId);
        return true;
    }

    public synchronized void release(String resource, long transactionId) {
        Set<Long> owners = sharedLocks.get(resource);
        if (owners != null) {
            owners.remove(transactionId);
            if (owners.isEmpty()) {
                sharedLocks.remove(resource);
            }
        }
        if (exclusiveLocks.get(resource) != null && exclusiveLocks.get(resource) == transactionId) {
            exclusiveLocks.remove(resource);
        }
    }
}
