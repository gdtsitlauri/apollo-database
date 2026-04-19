package com.apollo.storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RecoveryManager {
    private final WALManager walManager;

    public RecoveryManager(WALManager walManager) {
        this.walManager = walManager;
    }

    public RecoveryReport recover() {
        List<LogRecord> ordered = walManager.records().stream()
                .sorted(Comparator.comparingLong(LogRecord::getLsn))
                .toList();

        Set<Long> begun = new LinkedHashSet<>();
        Set<Long> committed = new LinkedHashSet<>();
        Set<Long> aborted = new LinkedHashSet<>();
        for (LogRecord record : ordered) {
            switch (record.getType()) {
                case BEGIN -> begun.add(record.getTransactionId());
                case COMMIT -> committed.add(record.getTransactionId());
                case ABORT -> aborted.add(record.getTransactionId());
                default -> {
                }
            }
        }

        Set<Long> inFlight = new LinkedHashSet<>(begun);
        inFlight.removeAll(committed);
        inFlight.removeAll(aborted);

        Map<String, String> state = new LinkedHashMap<>();
        List<String> redoSteps = new ArrayList<>();
        List<String> undoSteps = new ArrayList<>();

        for (LogRecord record : ordered) {
            if ((record.getType() == LogRecord.Type.UPDATE || record.getType() == LogRecord.Type.CLR)
                    && committed.contains(record.getTransactionId())) {
                applyRecord(state, record);
                redoSteps.add("REDO tx=" + record.getTransactionId() + " key=" + record.getKey() + " -> " + record.getAfterValue());
            }
        }

        for (int i = ordered.size() - 1; i >= 0; i--) {
            LogRecord record = ordered.get(i);
            if (record.getType() == LogRecord.Type.UPDATE
                    && (aborted.contains(record.getTransactionId()) || inFlight.contains(record.getTransactionId()))) {
                undoSteps.add("UNDO tx=" + record.getTransactionId() + " key=" + record.getKey() + " -> " + record.getBeforeValue());
            }
        }

        return new RecoveryReport(
                Map.copyOf(state),
                List.copyOf(redoSteps),
                List.copyOf(undoSteps),
                Set.copyOf(committed),
                Set.copyOf(aborted),
                Set.copyOf(inFlight));
    }

    private void applyRecord(Map<String, String> state, LogRecord record) {
        if (record.getAfterValue() == null) {
            state.remove(record.getKey());
        } else {
            state.put(record.getKey(), record.getAfterValue());
        }
    }

    public record RecoveryReport(
            Map<String, String> recoveredState,
            List<String> redoSteps,
            List<String> undoSteps,
            Set<Long> committedTransactions,
            Set<Long> abortedTransactions,
            Set<Long> inFlightTransactions
    ) {
    }
}
