package com.apollo.query;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.LinkedHashMap;
import java.util.Map;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DatabaseCatalogTest {
    @TempDir
    Path tempDir;

    @Test
    void testIndexedLookupTracksInsertUpdateDelete() {
        DatabaseCatalog catalog = new DatabaseCatalog();
        catalog.createTable("users", java.util.List.of("id", "name"));
        DatabaseCatalog.Table table = catalog.table("users");

        Map<String, String> row = new LinkedHashMap<>();
        row.put("id", "1");
        row.put("name", "alice");
        table.insertRow(row);

        assertEquals(1, table.lookupEquals("id", "1").size());

        table.updateRows(new AST.Predicate("id", "1"), Map.of("id", "2", "name", "alice-updated"));
        assertEquals(0, table.lookupEquals("id", "1").size());
        assertEquals("alice-updated", table.lookupEquals("id", "2").get(0).get("name"));

        table.deleteRows(new AST.Predicate("id", "2"));
        assertEquals(0, table.lookupEquals("id", "2").size());
    }

    @Test
    void testCatalogReloadsSchemaAndRowsFromDisk() {
        Path root = tempDir.resolve("catalog");

        DatabaseCatalog first = new DatabaseCatalog(root);
        first.createTable("users", java.util.List.of("id", "name"));
        DatabaseCatalog.Table firstTable = first.table("users");
        firstTable.insertRow(Map.of("id", "1", "name", "alice"));
        firstTable.insertRow(Map.of("id", "2", "name", "bob"));

        DatabaseCatalog second = new DatabaseCatalog(root);
        DatabaseCatalog.Table secondTable = second.table("users");
        assertEquals(2, secondTable.scanRows().size());
        assertEquals("alice", secondTable.lookupEquals("id", "1").get(0).get("name"));
        assertEquals("bob", secondTable.lookupEquals("id", "2").get(0).get("name"));
    }
}
