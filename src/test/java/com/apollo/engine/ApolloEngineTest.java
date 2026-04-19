package com.apollo.engine;

import com.apollo.query.DatabaseCatalog;
import com.apollo.transaction.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ApolloEngineTest {
    @TempDir
    Path tempDir;

    @Test
    void testWalBackedRecoveryFlow() {
        Path walPath = tempDir.resolve("engine.wal");
        ApolloEngine writer = new ApolloEngine(new DatabaseCatalog(), walPath);

        Transaction committed = writer.begin(Transaction.IsolationLevel.SNAPSHOT);
        writer.put(committed, "account:1", "100");
        writer.commit(committed);

        Transaction rolledBack = writer.begin(Transaction.IsolationLevel.SNAPSHOT);
        writer.put(rolledBack, "account:2", "200");
        writer.rollback(rolledBack);

        ApolloEngine recovered = new ApolloEngine(new DatabaseCatalog(), walPath);
        assertEquals("100", recovered.recoverKeyValueState().get("account:1"));
        assertFalse(recovered.recoverKeyValueState().containsKey("account:2"));
    }

    @Test
    void testSqlAndKvCanCoexist() {
        ApolloEngine engine = new ApolloEngine(new DatabaseCatalog(), tempDir.resolve("coexist.wal"));
        engine.executeSql("CREATE TABLE users (id, name)");
        engine.executeSql("INSERT INTO users VALUES (1, 'alice')");

        Transaction tx = engine.begin(Transaction.IsolationLevel.READ_COMMITTED);
        engine.put(tx, "meta:last_user", "1");
        engine.commit(tx);

        assertEquals("alice", engine.executeSql("SELECT * FROM users WHERE id = 1").rows().get(0).get("name"));
        assertEquals("1", engine.currentKeyValueState().get("meta:last_user"));
    }

    @Test
    void testTransactionalSqlMvccVisibility() {
        ApolloEngine engine = new ApolloEngine(new DatabaseCatalog(tempDir.resolve("mvcc-catalog")), tempDir.resolve("mvcc.wal"));
        engine.executeSql("CREATE TABLE users (id, name)");

        Transaction tx = engine.begin(Transaction.IsolationLevel.SNAPSHOT);
        engine.executeSql(tx, "INSERT INTO users VALUES (1, 'alice')");

        assertEquals(1, engine.executeSql(tx, "SELECT * FROM users WHERE id = 1").rows().size());
        assertEquals(0, engine.executeSql("SELECT * FROM users WHERE id = 1").rows().size());

        engine.commit(tx);
        assertEquals("alice", engine.executeSql("SELECT * FROM users WHERE id = 1").rows().get(0).get("name"));
    }

    @Test
    void testTransactionalSqlRollbackHidesAbortedVersions() {
        ApolloEngine engine = new ApolloEngine(new DatabaseCatalog(tempDir.resolve("rollback-catalog")), tempDir.resolve("rollback.wal"));
        engine.executeSql("CREATE TABLE users (id, name)");
        engine.executeSql("INSERT INTO users VALUES (1, 'alice')");

        Transaction tx = engine.begin(Transaction.IsolationLevel.SNAPSHOT);
        engine.executeSql(tx, "UPDATE users SET name = 'alice-updated' WHERE id = 1");
        assertEquals("alice-updated", engine.executeSql(tx, "SELECT * FROM users WHERE id = 1").rows().get(0).get("name"));

        engine.rollback(tx);
        assertEquals("alice", engine.executeSql("SELECT * FROM users WHERE id = 1").rows().get(0).get("name"));
    }
}
