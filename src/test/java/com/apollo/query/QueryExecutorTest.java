package com.apollo.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryExecutorTest {
    private final SQLParser parser = new SQLParser();
    private final QueryPlanner planner = new QueryPlanner();
    private final QueryExecutor executor = new QueryExecutor(new DatabaseCatalog());

    @Test
    void testCreateInsertSelectUpdateDelete() {
        execute("CREATE TABLE users (id, name)");
        execute("INSERT INTO users VALUES (1, 'alice')");
        execute("INSERT INTO users VALUES (2, 'bob')");

        QueryResult selected = execute("SELECT id, name FROM users WHERE id = 1");
        assertEquals(1, selected.rows().size());
        assertEquals("alice", selected.rows().get(0).get("name"));

        execute("UPDATE users SET name = 'alice-updated' WHERE id = 1");
        QueryResult updated = execute("SELECT * FROM users WHERE id = 1");
        assertEquals("alice-updated", updated.rows().get(0).get("name"));

        execute("DELETE FROM users WHERE id = 2");
        QueryResult remaining = execute("SELECT * FROM users");
        assertEquals(1, remaining.rows().size());
    }

    @Test
    void testJoinAndAggregate() {
        execute("CREATE TABLE users (id, name)");
        execute("CREATE TABLE orders (id, user_id, item, amount)");
        execute("INSERT INTO users VALUES (1, 'alice')");
        execute("INSERT INTO users VALUES (2, 'bob')");
        execute("INSERT INTO orders VALUES (10, 1, 'book', 20)");
        execute("INSERT INTO orders VALUES (11, 1, 'pen', 5)");
        execute("INSERT INTO orders VALUES (12, 2, 'mouse', 50)");

        QueryResult joined = execute("SELECT users.name, orders.item FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 1");
        assertEquals(2, joined.rows().size());
        assertEquals("alice", joined.rows().get(0).get("users.name"));

        QueryResult aggregate = execute("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id");
        assertEquals(2, aggregate.rows().size());
        assertEquals("2", aggregate.rows().get(0).get("COUNT(*)"));
        assertEquals("1", aggregate.rows().get(1).get("COUNT(*)"));

        QueryResult sum = execute("SELECT user_id, SUM(amount) FROM orders GROUP BY user_id ORDER BY user_id");
        assertEquals("25", sum.rows().get(0).get("SUM(amount)"));
        assertEquals("50", sum.rows().get(1).get("SUM(amount)"));
    }

    @Test
    void testOrderByAndLimit() {
        execute("CREATE TABLE metrics (id, name)");
        execute("INSERT INTO metrics VALUES (2, 'b')");
        execute("INSERT INTO metrics VALUES (1, 'a')");
        execute("INSERT INTO metrics VALUES (3, 'c')");

        QueryResult ordered = execute("SELECT id, name FROM metrics ORDER BY name DESC LIMIT 2");
        assertEquals(2, ordered.rows().size());
        assertEquals("c", ordered.rows().get(0).get("name"));
        assertEquals("b", ordered.rows().get(1).get("name"));
    }

    private QueryResult execute(String sql) {
        AST ast = parser.parse(sql);
        return executor.execute(planner.buildPhysical(planner.buildLogical(ast), ast));
    }
}
