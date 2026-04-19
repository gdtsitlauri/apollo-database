package com.apollo.transaction;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransactionManagerTest {
    @Test
    void testRollbackToSavepoint() {
        TransactionManager manager = new TransactionManager();
        Transaction tx = manager.begin(Transaction.IsolationLevel.SNAPSHOT);

        manager.write(tx, "users:1", "alice");
        manager.createSavepoint(tx, "before-email");
        manager.write(tx, "users:1", "alice-updated");
        manager.rollbackToSavepoint(tx, "before-email");
        manager.commit(tx);

        assertEquals("alice", manager.committedState().get("users:1"));
        assertEquals(Transaction.State.COMMITTED, tx.getState());
    }

    @Test
    void testRollbackDiscardChanges() {
        TransactionManager manager = new TransactionManager();
        Transaction tx = manager.begin(Transaction.IsolationLevel.READ_COMMITTED);

        manager.write(tx, "users:2", "bob");
        manager.rollback(tx);

        assertEquals(null, manager.committedState().get("users:2"));
        assertEquals(Transaction.State.ROLLED_BACK, tx.getState());
    }
}
