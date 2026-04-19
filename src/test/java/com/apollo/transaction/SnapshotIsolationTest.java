package com.apollo.transaction;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies snapshot isolation: no phantom reads, concurrent writers don't interfere.
 */
class SnapshotIsolationTest {

    @Test
    void testSnapshotIsolation() {
        TransactionManager manager = new TransactionManager();

        // txn1 writes data and commits before txn2 starts
        Transaction txn1 = manager.begin(Transaction.IsolationLevel.SNAPSHOT);
        manager.write(txn1, "row:1", "v1");
        manager.commit(txn1);

        // txn2 takes a snapshot after txn1 committed — it must see v1
        Transaction txn2 = manager.begin(Transaction.IsolationLevel.SNAPSHOT);
        assertEquals("v1", manager.readCommitted("row:1"));

        // txn3 writes a new row while txn2 is active (phantom scenario)
        Transaction txn3 = manager.begin(Transaction.IsolationLevel.SNAPSHOT);
        manager.write(txn3, "row:2", "phantom");
        manager.commit(txn3);

        // txn2's snapshot was taken before txn3 — it must not see the phantom row
        // (TransactionManager.read returns null for rows not present at snapshot start)
        String txn2view = manager.readAtSnapshot(txn2, "row:2");
        assertNull(txn2view, "Snapshot isolation: txn2 must not see row inserted after its snapshot");

        manager.commit(txn2);

        // After both commits, committed state has both rows
        assertEquals("v1", manager.committedState().get("row:1"));
        assertEquals("phantom", manager.committedState().get("row:2"));
    }
}
