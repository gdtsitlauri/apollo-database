package com.apollo.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Map;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class WALManagerTest {
    @Test
    void testWalRecovery() {
        WALManager wal = new WALManager();

        wal.begin(1);
        wal.update(1, "users:1", null, "alice");
        wal.commit(1);

        wal.begin(2);
        wal.update(2, "users:2", null, "bob");
        wal.abort(2);

        Map<String, String> state = wal.recover();
        assertEquals("alice", state.get("users:1"));
        assertFalse(state.containsKey("users:2"));
    }

    @TempDir
    Path tempDir;

    @Test
    void testWalPersistsAcrossRestart() {
        Path walFile = tempDir.resolve("apollo.wal");
        WALManager writer = new WALManager(walFile);
        writer.begin(10);
        writer.update(10, "accounts:1", null, "100");
        writer.commit(10);
        writer.force();

        WALManager reader = new WALManager(walFile);
        Map<String, String> state = reader.recover();
        assertEquals("100", state.get("accounts:1"));
    }
}
