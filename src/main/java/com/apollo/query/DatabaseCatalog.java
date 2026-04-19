package com.apollo.query;

import com.apollo.storage.BPlusTree;
import com.apollo.storage.BufferPool;
import com.apollo.storage.HeapFile;
import com.apollo.storage.PageManager;
import com.apollo.storage.Record;
import com.apollo.storage.RecordId;
import com.apollo.transaction.MVCCManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DatabaseCatalog {
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static final int DEFAULT_BUFFER_POOL_SIZE = 64;

    private final Map<String, Table> tables = new LinkedHashMap<>();
    private final Path storageRoot;

    public DatabaseCatalog() {
        this(null);
    }

    public DatabaseCatalog(Path storageRoot) {
        this.storageRoot = storageRoot;
        loadSchemaIfPresent();
    }

    public void createTable(String name, List<String> columns) {
        if (tables.containsKey(name)) {
            throw new IllegalArgumentException("Table already exists " + name);
        }
        tables.put(name, new Table(name, List.copyOf(columns), storageRoot == null ? null : tablePath(name)));
        persistSchema();
    }

    public Table table(String name) {
        Table table = tables.get(name);
        if (table == null) {
            throw new IllegalArgumentException("Unknown table " + name);
        }
        return table;
    }

    private void loadSchemaIfPresent() {
        if (storageRoot == null) {
            return;
        }
        Path schema = schemaPath();
        if (!Files.exists(schema)) {
            return;
        }
        try {
            for (String line : Files.readAllLines(schema)) {
                if (line.isBlank()) {
                    continue;
                }
                String[] parts = line.split("\\|", 2);
                String name = parts[0];
                List<String> columns = parts.length > 1 && !parts[1].isBlank()
                        ? List.of(parts[1].split(","))
                        : List.of();
                tables.put(name, new Table(name, columns, tablePath(name)));
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to load schema from " + schema, exception);
        }
    }

    private void persistSchema() {
        if (storageRoot == null) {
            return;
        }
        try {
            Files.createDirectories(storageRoot);
            List<String> lines = tables.values().stream()
                    .map(table -> table.name() + "|" + String.join(",", table.columns()))
                    .toList();
            Files.write(schemaPath(), lines);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to persist catalog schema", exception);
        }
    }

    private Path schemaPath() {
        return storageRoot.resolve("schema.catalog");
    }

    private Path tablePath(String name) {
        return storageRoot.resolve(name + ".pages");
    }

    public static final class Table {
        private final String name;
        private final List<String> columns;
        private final String indexedColumn;
        private final BPlusTree<String, List<RowVersion>> index;
        private final HeapFile heapFile;
        private final MVCCManager mvccManager = new MVCCManager();
        private final List<RowVersion> versions = new ArrayList<>();
        private long nextSequence = 1;

        private Table(String name, List<String> columns, Path heapPath) {
            this.name = Objects.requireNonNull(name, "name");
            this.columns = List.copyOf(columns);
            this.indexedColumn = columns.isEmpty() ? null : columns.get(0);
            this.index = indexedColumn == null ? null : new BPlusTree<>(8);
            if (heapPath == null) {
                this.heapFile = null;
            } else {
                PageManager pageManager = new PageManager(DEFAULT_PAGE_SIZE, heapPath);
                BufferPool bufferPool = new BufferPool(DEFAULT_BUFFER_POOL_SIZE, pageManager);
                this.heapFile = new HeapFile(pageManager, bufferPool);
            }
            if (heapFile != null) {
                loadVersionsFromHeap();
            }
        }

        public String name() {
            return name;
        }

        public List<String> columns() {
            return columns;
        }

        public List<Map<String, String>> rows() {
            return scanRows();
        }

        public String indexedColumn() {
            return indexedColumn;
        }

        public void insertRow(Map<String, String> row) {
            insertRow(row, null);
        }

        public void insertRow(Map<String, String> row, QuerySession session) {
            appendVersion(new LinkedHashMap<>(row), transactionId(session));
            rebuildIndex();
        }

        public int deleteRows(AST.Predicate predicate) {
            return deleteRows(predicate, null);
        }

        public int deleteRows(AST.Predicate predicate, QuerySession session) {
            long txId = transactionId(session);
            int deleted = 0;
            for (RowVersion version : visibleRowVersions(session).values()) {
                if (predicate == null || Objects.equals(resolveValue(version.row(), predicate.column()), predicate.value())) {
                    if (version.record().getXmax() == null || !version.record().getXmax().equals(txId)) {
                        version.record().setXmax(txId);
                        persistVersion(version);
                        deleted++;
                    }
                }
            }
            if (deleted > 0) {
                rebuildIndex();
            }
            return deleted;
        }

        public int updateRows(AST.Predicate predicate, Map<String, String> assignments) {
            return updateRows(predicate, assignments, null);
        }

        public int updateRows(AST.Predicate predicate, Map<String, String> assignments, QuerySession session) {
            long txId = transactionId(session);
            int updated = 0;
            List<RowVersion> targets = new ArrayList<>(visibleRowVersions(session).values());
            for (RowVersion version : targets) {
                if (predicate == null || Objects.equals(resolveValue(version.row(), predicate.column()), predicate.value())) {
                    if (version.record().getXmax() == null || !version.record().getXmax().equals(txId)) {
                        version.record().setXmax(txId);
                        persistVersion(version);
                    }
                    Map<String, String> newRow = new LinkedHashMap<>(version.row());
                    newRow.putAll(assignments);
                    appendVersion(newRow, txId);
                    updated++;
                }
            }
            if (updated > 0) {
                rebuildIndex();
            }
            return updated;
        }

        public List<Map<String, String>> scanRows() {
            return scanRows(null);
        }

        public List<Map<String, String>> scanRows(QuerySession session) {
            return visibleRowVersions(session).values().stream()
                    .map(RowVersion::row)
                    .map(LinkedHashMap::new)
                    .<Map<String, String>>map(row -> row)
                    .toList();
        }

        public List<Map<String, String>> lookupEquals(String column, String value) {
            return lookupEquals(column, value, null);
        }

        public List<Map<String, String>> lookupEquals(String column, String value, QuerySession session) {
            if (index == null || !Objects.equals(indexedColumn, column)) {
                return scanRows(session).stream()
                        .filter(row -> Objects.equals(resolveValue(row, column), value))
                        .toList();
            }
            return visibleVersions(index.search(value).orElse(List.of()), session).values().stream()
                    .map(RowVersion::row)
                    .map(LinkedHashMap::new)
                    .<Map<String, String>>map(row -> row)
                    .toList();
        }

        public Statistics statistics() {
            return new Statistics(scanRows().size(), columns.size());
        }

        private void appendVersion(Map<String, String> row, long txId) {
            Record record = serializeRow(row);
            record.setXmin(txId);
            RecordId recordId = heapFile == null ? null : heapFile.insert(record);
            versions.add(new RowVersion(nextSequence++, recordId, record, row));
        }

        private void loadVersionsFromHeap() {
            versions.clear();
            for (HeapFile.RecordEntry entry : heapFile.scanEntries()) {
                versions.add(new RowVersion(nextSequence++, entry.recordId(), entry.record(), deserializeRow(entry.record().getData())));
            }
            rebuildIndex();
        }

        private void persistVersion(RowVersion version) {
            if (heapFile != null && version.recordId() != null) {
                heapFile.update(version.recordId(), version.record());
            }
        }

        private LinkedHashMap<String, RowVersion> visibleRowVersions(QuerySession session) {
            return visibleVersions(versions, session);
        }

        private LinkedHashMap<String, RowVersion> visibleVersions(List<RowVersion> source, QuerySession session) {
            Set<Long> committed = committedTransactions(session);
            Set<Long> aborted = abortedTransactions(session);
            var snapshot = session == null ? QuerySession.autocommit(Set.of(0L), Set.of()).snapshot() : session.snapshot();

            LinkedHashMap<String, RowVersion> visible = new LinkedHashMap<>();
            for (RowVersion version : source) {
                if (mvccManager.isVisible(version.record(), snapshot, committed, aborted)) {
                    visible.put(primaryKey(version.row()), version);
                }
            }
            return visible;
        }

        private Set<Long> committedTransactions(QuerySession session) {
            if (session == null) {
                return Set.of(0L);
            }
            Set<Long> committed = new LinkedHashSet<>(session.committedTransactions());
            committed.add(0L);
            return Set.copyOf(committed);
        }

        private Set<Long> abortedTransactions(QuerySession session) {
            return session == null ? Set.of() : session.abortedTransactions();
        }

        private long transactionId(QuerySession session) {
            return session == null || session.transaction() == null ? 0L : session.transaction().getTransactionId();
        }

        private String primaryKey(Map<String, String> row) {
            return indexedColumn == null ? Integer.toString(row.hashCode()) : row.get(indexedColumn);
        }

        private Record serializeRow(Map<String, String> row) {
            try {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes);
                output.writeInt(columns.size());
                for (String column : columns) {
                    output.writeUTF(column);
                    output.writeUTF(row.getOrDefault(column, ""));
                }
                output.flush();
                return new Record(bytes.toByteArray());
            } catch (IOException exception) {
                throw new IllegalStateException("Failed to serialize row", exception);
            }
        }

        private Map<String, String> deserializeRow(byte[] bytes) {
            try {
                DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
                int count = input.readInt();
                Map<String, String> row = new LinkedHashMap<>();
                for (int i = 0; i < count; i++) {
                    row.put(input.readUTF(), input.readUTF());
                }
                return row;
            } catch (IOException exception) {
                throw new IllegalStateException("Failed to deserialize row", exception);
            }
        }

        private void rebuildIndex() {
            if (index == null) {
                return;
            }
            Map<String, List<RowVersion>> grouped = new LinkedHashMap<>();
            for (RowVersion version : versions) {
                grouped.computeIfAbsent(primaryKey(version.row()), ignored -> new ArrayList<>()).add(version);
            }
            Map<String, List<RowVersion>> immutableGrouped = new LinkedHashMap<>();
            for (Map.Entry<String, List<RowVersion>> entry : grouped.entrySet()) {
                immutableGrouped.put(entry.getKey(), List.copyOf(entry.getValue()));
            }
            index.putAll(immutableGrouped);
        }

        private String resolveValue(Map<String, String> row, String column) {
            if (row.containsKey(column)) {
                return row.get(column);
            }
            int dot = column.indexOf('.');
            if (dot >= 0) {
                return row.get(column.substring(dot + 1));
            }
            return row.get(column);
        }
    }

    public record RowVersion(long sequence, RecordId recordId, Record record, Map<String, String> row) {
    }
}
