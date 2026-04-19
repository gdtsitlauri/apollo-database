package com.apollo.transaction;

import java.util.LinkedHashMap;
import java.util.Map;

public class Transaction {
    public enum IsolationLevel {
        READ_COMMITTED,
        SNAPSHOT,
        SERIALIZABLE
    }

    public enum State {
        ACTIVE,
        COMMITTED,
        ROLLED_BACK,
        ABORTED
    }

    private final long transactionId;
    private final IsolationLevel isolationLevel;
    private final LinkedHashMap<String, String> stagedChanges = new LinkedHashMap<>();
    private final LinkedHashMap<String, LinkedHashMap<String, String>> savepoints = new LinkedHashMap<>();
    private Map<String, String> snapshotState = Map.of();
    private State state = State.ACTIVE;

    public Transaction(long transactionId, IsolationLevel isolationLevel) {
        this.transactionId = transactionId;
        this.isolationLevel = isolationLevel;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public State getState() {
        return state;
    }

    public void stageChange(String key, String value) {
        ensureActive();
        stagedChanges.put(key, value);
    }

    public Map<String, String> getStagedChanges() {
        return Map.copyOf(stagedChanges);
    }

    public void setSnapshotState(Map<String, String> state) {
        this.snapshotState = Map.copyOf(state);
    }

    public Map<String, String> getSnapshotState() {
        return snapshotState;
    }

    public void createSavepoint(String name) {
        ensureActive();
        savepoints.put(name, new LinkedHashMap<>(stagedChanges));
    }

    public void rollbackToSavepoint(String name) {
        ensureActive();
        LinkedHashMap<String, String> snapshot = savepoints.get(name);
        if (snapshot == null) {
            throw new IllegalArgumentException("Unknown savepoint " + name);
        }
        stagedChanges.clear();
        stagedChanges.putAll(snapshot);
    }

    public void markCommitted() {
        ensureActive();
        state = State.COMMITTED;
    }

    public void markRolledBack() {
        ensureActive();
        stagedChanges.clear();
        state = State.ROLLED_BACK;
    }

    public void markAborted() {
        ensureActive();
        stagedChanges.clear();
        state = State.ABORTED;
    }

    private void ensureActive() {
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Transaction " + transactionId + " is not active");
        }
    }
}
