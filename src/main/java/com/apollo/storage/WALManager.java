package com.apollo.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class WALManager {
    private final AtomicLong lsnGenerator = new AtomicLong(1);
    private final List<LogRecord> log = new ArrayList<>();
    private final Path logFile;

    public WALManager() {
        this(null);
    }

    public WALManager(Path logFile) {
        this.logFile = logFile;
        loadIfPresent();
    }

    public synchronized long begin(long transactionId) {
        return append(transactionId, LogRecord.Type.BEGIN, null, null, null, null).getLsn();
    }

    public synchronized long update(long transactionId, String key, String beforeValue, String afterValue) {
        return append(transactionId, LogRecord.Type.UPDATE, key, beforeValue, afterValue, null).getLsn();
    }

    public synchronized long commit(long transactionId) {
        return append(transactionId, LogRecord.Type.COMMIT, null, null, null, null).getLsn();
    }

    public synchronized long abort(long transactionId) {
        return append(transactionId, LogRecord.Type.ABORT, null, null, null, null).getLsn();
    }

    public synchronized long compensation(long transactionId, String key, String beforeValue, String afterValue, Long undoNextLsn) {
        return append(transactionId, LogRecord.Type.CLR, key, beforeValue, afterValue, undoNextLsn).getLsn();
    }

    public synchronized List<LogRecord> records() {
        return List.copyOf(log);
    }

    public synchronized Map<String, String> recover() {
        Set<Long> committed = log.stream()
                .filter(record -> record.getType() == LogRecord.Type.COMMIT)
                .map(LogRecord::getTransactionId)
                .collect(java.util.stream.Collectors.toSet());

        Map<String, String> state = new HashMap<>();
        log.stream()
                .sorted(Comparator.comparingLong(LogRecord::getLsn))
                .filter(record -> record.getType() == LogRecord.Type.UPDATE || record.getType() == LogRecord.Type.CLR)
                .filter(record -> committed.contains(record.getTransactionId()))
                .forEach(record -> {
                    if (record.getAfterValue() == null) {
                        state.remove(record.getKey());
                    } else {
                        state.put(record.getKey(), record.getAfterValue());
                    }
                });
        return state;
    }

    public synchronized void force() {
        persist();
    }

    private LogRecord append(long transactionId, LogRecord.Type type, String key, String beforeValue, String afterValue, Long undoNextLsn) {
        LogRecord record = new LogRecord(lsnGenerator.getAndIncrement(), transactionId, type, key, beforeValue, afterValue, undoNextLsn);
        log.add(record);
        persist();
        return record;
    }

    private void loadIfPresent() {
        if (logFile == null || !Files.exists(logFile)) {
            return;
        }
        try {
            List<String> lines = Files.readAllLines(logFile);
            long maxLsn = 0;
            for (String line : lines) {
                if (line.isBlank()) {
                    continue;
                }
                String[] parts = line.split("\\|", -1);
                long lsn = Long.parseLong(parts[0]);
                long transactionId = Long.parseLong(parts[1]);
                LogRecord.Type type = LogRecord.Type.valueOf(parts[2]);
                String key = emptyToNull(parts[3]);
                String beforeValue = emptyToNull(parts[4]);
                String afterValue = emptyToNull(parts[5]);
                Long undoNextLsn = parts.length > 6 && !parts[6].isEmpty() ? Long.parseLong(parts[6]) : null;
                log.add(new LogRecord(lsn, transactionId, type, key, beforeValue, afterValue, undoNextLsn));
                maxLsn = Math.max(maxLsn, lsn);
            }
            lsnGenerator.set(maxLsn + 1);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to load WAL file " + logFile, exception);
        }
    }

    private void persist() {
        if (logFile == null) {
            return;
        }
        try {
            Path parent = logFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            List<String> lines = log.stream()
                    .map(record -> String.join("|",
                            Long.toString(record.getLsn()),
                            Long.toString(record.getTransactionId()),
                            record.getType().name(),
                            nullToEmpty(record.getKey()),
                            nullToEmpty(record.getBeforeValue()),
                            nullToEmpty(record.getAfterValue()),
                            record.getUndoNextLsn() == null ? "" : Long.toString(record.getUndoNextLsn())))
                    .toList();
            Files.write(logFile, lines);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to persist WAL file " + logFile, exception);
        }
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }
}
