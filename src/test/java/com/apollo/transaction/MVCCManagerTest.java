package com.apollo.transaction;

import com.apollo.storage.Record;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MVCCManagerTest {
    @Test
    void testMvccIsolation() {
        MVCCManager manager = new MVCCManager();
        Record record = Record.fromString("v1");
        record.setXmin(1);

        TransactionSnapshot snapshot = new TransactionSnapshot(2L, Set.of(2L));
        assertTrue(manager.isVisible(record, snapshot, Set.of(1L), Set.of()));

        record.setXmax(3L);
        assertTrue(manager.isVisible(record, snapshot, Set.of(1L), Set.of()));

        TransactionSnapshot laterSnapshot = new TransactionSnapshot(4L, Set.of(4L));
        assertFalse(manager.isVisible(record, laterSnapshot, Set.of(1L, 3L), Set.of()));
    }
}
