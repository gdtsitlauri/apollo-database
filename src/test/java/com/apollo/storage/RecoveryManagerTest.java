package com.apollo.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecoveryManagerTest {
    @Test
    void testRecoveryReportCapturesRedoAndUndo() {
        WALManager wal = new WALManager();
        wal.begin(1);
        wal.update(1, "users:1", null, "alice");
        wal.commit(1);

        wal.begin(2);
        wal.update(2, "users:2", null, "bob");
        wal.abort(2);

        wal.begin(3);
        wal.update(3, "users:3", null, "carol");

        RecoveryManager.RecoveryReport report = new RecoveryManager(wal).recover();

        assertEquals("alice", report.recoveredState().get("users:1"));
        assertTrue(report.redoSteps().stream().anyMatch(step -> step.contains("users:1")));
        assertTrue(report.undoSteps().stream().anyMatch(step -> step.contains("users:2")));
        assertTrue(report.undoSteps().stream().anyMatch(step -> step.contains("users:3")));
        assertTrue(report.abortedTransactions().contains(2L));
        assertTrue(report.inFlightTransactions().contains(3L));
    }
}
