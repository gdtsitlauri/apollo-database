package com.apollo.transaction;

import com.apollo.storage.Record;

import java.util.Objects;
import java.util.Set;

public class MVCCManager {
    public boolean isVisible(Record record, TransactionSnapshot snapshot, Set<Long> committedTransactions, Set<Long> abortedTransactions) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(snapshot, "snapshot");
        if (record.getXmin() == snapshot.transactionId()) {
            return record.getXmax() == null || !record.getXmax().equals(snapshot.transactionId());
        }
        if (!committedTransactions.contains(record.getXmin())) {
            return false;
        }
        Long xmax = record.getXmax();
        if (xmax == null) {
            return true;
        }
        if (xmax.equals(snapshot.transactionId())) {
            return false;
        }
        if (abortedTransactions.contains(xmax)) {
            return true;
        }
        if (snapshot.activeTransactions().contains(xmax)) {
            return true;
        }
        return !committedTransactions.contains(xmax) || xmax > snapshot.transactionId();
    }
}
