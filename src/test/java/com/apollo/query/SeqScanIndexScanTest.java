package com.apollo.query;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Named tests for sequential scan and index scan operators.
 */
class SeqScanIndexScanTest {

    private final SQLParser parser = new SQLParser();
    private final QueryPlanner planner = new QueryPlanner();
    private final DatabaseCatalog catalog = new DatabaseCatalog();
    private final QueryExecutor executor = new QueryExecutor(catalog);

    private QueryResult execute(String sql) {
        AST ast = parser.parse(sql);
        return executor.execute(planner.buildPhysical(planner.buildLogical(ast), ast));
    }

    @Test
    void testSeqScan() {
        execute("CREATE TABLE products (id, name, price)");
        execute("INSERT INTO products VALUES (1, 'apple', 10)");
        execute("INSERT INTO products VALUES (2, 'banana', 5)");
        execute("INSERT INTO products VALUES (3, 'cherry', 20)");

        // Full table scan — must return all 3 rows
        QueryResult result = execute("SELECT * FROM products");
        assertEquals(3, result.rows().size());

        List<String> names = result.rows().stream()
                .map(r -> r.get("name"))
                .toList();
        assertTrue(names.contains("apple"));
        assertTrue(names.contains("banana"));
        assertTrue(names.contains("cherry"));
    }

    @Test
    void testIndexScan() {
        execute("CREATE TABLE employees (id, name, dept)");
        for (int i = 1; i <= 100; i++) {
            execute("INSERT INTO employees VALUES (" + i + ", 'emp" + i + "', 'dept" + (i % 5) + "')");
        }

        // Point lookup by primary key — planner may use index scan when index is available
        QueryResult result = execute("SELECT id, name FROM employees WHERE id = 42");
        assertEquals(1, result.rows().size());
        assertEquals("emp42", result.rows().get(0).get("name"));

        // Range with WHERE — exercises filter path (index or seq scan with predicate)
        QueryResult rangeResult = execute("SELECT id FROM employees WHERE id = 1");
        assertEquals(1, rangeResult.rows().size());
        assertEquals("1", rangeResult.rows().get(0).get("id"));
    }
}
