package com.apollo.showcase;

import com.apollo.engine.ApolloEngine;
import com.apollo.query.AST;
import com.apollo.query.DatabaseCatalog;
import com.apollo.query.QueryExecutor;
import com.apollo.query.QueryPlanner;
import com.apollo.query.QueryResult;
import com.apollo.query.SQLParser;
import com.apollo.storage.BPlusTree;
import com.apollo.transaction.Transaction;

import java.nio.file.Path;

public class Main {
    public static void main(String[] args) {
        BPlusTree<Integer, String> tree = new BPlusTree<>(4);
        tree.insert(10, "ten");
        tree.insert(20, "twenty");
        tree.insert(30, "thirty");

        SQLParser parser = new SQLParser();
        QueryPlanner planner = new QueryPlanner();
        QueryExecutor executor = new QueryExecutor(new DatabaseCatalog());

        run(parser, planner, executor, "CREATE TABLE users (id, name)");
        run(parser, planner, executor, "INSERT INTO users VALUES (1, 'alice')");
        QueryResult result = run(parser, planner, executor, "SELECT * FROM users WHERE id = 1");

        ApolloEngine engine = new ApolloEngine(new DatabaseCatalog(), Path.of("results", "showcase", "apollo.wal"));
        Transaction tx = engine.begin(Transaction.IsolationLevel.SNAPSHOT);
        engine.put(tx, "users:1", "alice");
        engine.createSavepoint(tx, "after-user");
        engine.put(tx, "users:2", "bob");
        engine.rollbackToSavepoint(tx, "after-user");
        engine.commit(tx);

        RecoveryShowcase recoveryShowcase = new RecoveryShowcase();
        PlanComparisonShowcase planShowcase = new PlanComparisonShowcase();

        System.out.println("APOLLO showcase tree height: " + tree.height());
        System.out.println("APOLLO showcase query rows: " + result.rows());
        System.out.println("APOLLO showcase kv state: " + engine.currentKeyValueState());
        System.out.println("APOLLO showcase recovery: " + recoveryShowcase.run(Path.of("results", "showcase", "recovery.wal")));
        System.out.println("APOLLO showcase plans: " + planShowcase.compare("SELECT users.name, orders.item FROM users JOIN orders ON users.id = orders.user_id"));

        MVCCVersionChainShowcase mvccShowcase = new MVCCVersionChainShowcase();
        System.out.println(mvccShowcase.run());
    }

    private static QueryResult run(SQLParser parser, QueryPlanner planner, QueryExecutor executor, String sql) {
        AST ast = parser.parse(sql);
        return executor.execute(planner.buildPhysical(planner.buildLogical(ast), ast));
    }
}
