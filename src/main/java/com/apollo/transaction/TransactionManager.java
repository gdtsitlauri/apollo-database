package com.apollo.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
    private final AtomicLong idGenerator = new AtomicLong(1);
    private final Set<Long> activeTransactions = new HashSet<>();
    private final Set<Long> committedTransactions = new HashSet<>(Set.of(0L));
    private final Set<Long> abortedTransactions = new HashSet<>();
    private final Map<String, String> committedState = new HashMap<>();

    public synchronized Transaction begin(Transaction.IsolationLevel isolationLevel) {
        long id = idGenerator.getAndIncrement();
        activeTransactions.add(id);
        Transaction tx = new Transaction(id, isolationLevel);
        if (isolationLevel == Transaction.IsolationLevel.SNAPSHOT) {
            tx.setSnapshotState(Map.copyOf(committedState));
        }
        return tx;
    }

    public synchronized TransactionSnapshot snapshot(Transaction transaction) {
        return new TransactionSnapshot(transaction.getTransactionId(), Set.copyOf(activeTransactions));
    }

    public synchronized void write(Transaction transaction, String key, String value) {
        transaction.stageChange(key, value);
    }

    public synchronized String read(Transaction transaction, String key) {
        if (transaction.getStagedChanges().containsKey(key)) {
            return transaction.getStagedChanges().get(key);
        }
        return committedState.get(key);
    }

    public synchronized void createSavepoint(Transaction transaction, String name) {
        transaction.createSavepoint(name);
    }

    public synchronized void rollbackToSavepoint(Transaction transaction, String name) {
        transaction.rollbackToSavepoint(name);
    }

    public synchronized Map<String, String> commit(Transaction transaction) {
        committedState.putAll(transaction.getStagedChanges());
        transaction.markCommitted();
        activeTransactions.remove(transaction.getTransactionId());
        committedTransactions.add(transaction.getTransactionId());
        abortedTransactions.remove(transaction.getTransactionId());
        return Map.copyOf(committedState);
    }

    public synchronized void rollback(Transaction transaction) {
        transaction.markRolledBack();
        activeTransactions.remove(transaction.getTransactionId());
        abortedTransactions.add(transaction.getTransactionId());
    }

    public synchronized void abort(Transaction transaction) {
        transaction.markAborted();
        activeTransactions.remove(transaction.getTransactionId());
        abortedTransactions.add(transaction.getTransactionId());
    }

    public synchronized String readCommitted(String key) {
        return committedState.get(key);
    }

    public synchronized String readAtSnapshot(Transaction transaction, String key) {
        return transaction.getSnapshotState().get(key);
    }

    public synchronized Map<String, String> committedState() {
        return Map.copyOf(committedState);
    }

    public synchronized Set<Long> committedTransactions() {
        return Set.copyOf(committedTransactions);
    }

    public synchronized Set<Long> abortedTransactions() {
        return Set.copyOf(abortedTransactions);
    }
}
