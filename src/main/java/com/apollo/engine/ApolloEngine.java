package com.apollo.engine;

import com.apollo.query.AST;
import com.apollo.query.DatabaseCatalog;
import com.apollo.query.PhysicalPlan;
import com.apollo.query.QueryExecutor;
import com.apollo.query.QueryPlanner;
import com.apollo.query.QueryResult;
import com.apollo.query.QuerySession;
import com.apollo.query.SQLParser;
import com.apollo.storage.WALManager;
import com.apollo.transaction.Transaction;
import com.apollo.transaction.TransactionManager;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public class ApolloEngine {
    private final SQLParser parser;
    private final QueryPlanner planner;
    private final QueryExecutor executor;
    private final TransactionManager transactionManager;
    private final WALManager walManager;
    private final Map<String, String> keyValueState = new LinkedHashMap<>();

    public ApolloEngine() {
        this(new DatabaseCatalog(), null);
    }

    public ApolloEngine(DatabaseCatalog catalog, Path walPath) {
        this.parser = new SQLParser();
        this.planner = new QueryPlanner();
        this.executor = new QueryExecutor(catalog);
        this.transactionManager = new TransactionManager();
        this.walManager = new WALManager(walPath);
        this.keyValueState.putAll(this.walManager.recover());
    }

    public QueryResult executeSql(String sql) {
        AST ast = parser.parse(sql);
        PhysicalPlan plan = planner.buildPhysical(planner.buildLogical(ast), ast);
        return executor.execute(plan, QuerySession.autocommit(transactionManager.committedTransactions(), transactionManager.abortedTransactions()));
    }

    public QueryResult executeSql(Transaction transaction, String sql) {
        AST ast = parser.parse(sql);
        PhysicalPlan plan = planner.buildPhysical(planner.buildLogical(ast), ast);
        QuerySession session = new QuerySession(
                transaction,
                transactionManager.snapshot(transaction),
                transactionManager.committedTransactions(),
                transactionManager.abortedTransactions());
        return executor.execute(plan, session);
    }

    public Transaction begin(Transaction.IsolationLevel isolationLevel) {
        Transaction transaction = transactionManager.begin(isolationLevel);
        walManager.begin(transaction.getTransactionId());
        return transaction;
    }

    public void put(Transaction transaction, String key, String value) {
        String beforeValue = keyValueState.containsKey(key)
                ? keyValueState.get(key)
                : transactionManager.read(transaction, key);
        transactionManager.write(transaction, key, value);
        walManager.update(transaction.getTransactionId(), key, beforeValue, value);
    }

    public void delete(Transaction transaction, String key) {
        String beforeValue = keyValueState.containsKey(key)
                ? keyValueState.get(key)
                : transactionManager.read(transaction, key);
        transactionManager.write(transaction, key, null);
        walManager.update(transaction.getTransactionId(), key, beforeValue, null);
    }

    public String get(Transaction transaction, String key) {
        if (transaction == null) {
            return keyValueState.get(key);
        }
        String staged = transactionManager.read(transaction, key);
        return staged != null || transaction.getStagedChanges().containsKey(key) ? staged : keyValueState.get(key);
    }

    public void createSavepoint(Transaction transaction, String name) {
        transactionManager.createSavepoint(transaction, name);
    }

    public void rollbackToSavepoint(Transaction transaction, String name) {
        transactionManager.rollbackToSavepoint(transaction, name);
    }

    public Map<String, String> commit(Transaction transaction) {
        Map<String, String> committed = transactionManager.commit(transaction);
        keyValueState.clear();
        keyValueState.putAll(compacted(committed));
        walManager.commit(transaction.getTransactionId());
        walManager.force();
        return Map.copyOf(keyValueState);
    }

    public void rollback(Transaction transaction) {
        for (Map.Entry<String, String> entry : transaction.getStagedChanges().entrySet()) {
            walManager.compensation(transaction.getTransactionId(), entry.getKey(), entry.getValue(), null, null);
        }
        transactionManager.rollback(transaction);
        walManager.abort(transaction.getTransactionId());
        walManager.force();
    }

    public Map<String, String> recoverKeyValueState() {
        keyValueState.clear();
        keyValueState.putAll(walManager.recover());
        return Map.copyOf(keyValueState);
    }

    public Map<String, String> currentKeyValueState() {
        return Map.copyOf(keyValueState);
    }

    public WALManager walManager() {
        return walManager;
    }

    private Map<String, String> compacted(Map<String, String> committed) {
        Map<String, String> compacted = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : committed.entrySet()) {
            if (entry.getValue() != null) {
                compacted.put(entry.getKey(), entry.getValue());
            }
        }
        return compacted;
    }
}
