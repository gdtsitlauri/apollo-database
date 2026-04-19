package com.apollo.showcase;

import com.apollo.engine.ApolloEngine;
import com.apollo.storage.RecoveryManager;
import com.apollo.transaction.Transaction;

import java.nio.file.Path;

public class RecoveryShowcase {
    public String run(Path walPath) {
        ApolloEngine writer = new ApolloEngine(new com.apollo.query.DatabaseCatalog(), walPath);
        Transaction first = writer.begin(Transaction.IsolationLevel.SNAPSHOT);
        writer.put(first, "users:1", "alice");
        writer.commit(first);

        Transaction second = writer.begin(Transaction.IsolationLevel.SNAPSHOT);
        writer.put(second, "users:2", "bob");
        writer.rollback(second);

        ApolloEngine recovered = new ApolloEngine(new com.apollo.query.DatabaseCatalog(), walPath);
        RecoveryManager.RecoveryReport report = new RecoveryManager(recovered.walManager()).recover();
        return "Recovered keys=" + report.recoveredState()
                + ", redo=" + report.redoSteps()
                + ", undo=" + report.undoSteps();
    }
}
