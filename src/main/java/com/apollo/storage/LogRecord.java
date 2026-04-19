package com.apollo.storage;

public class LogRecord {
    public enum Type {
        BEGIN,
        COMMIT,
        ABORT,
        UPDATE,
        CLR
    }

    private final long lsn;
    private final long transactionId;
    private final Type type;
    private final String key;
    private final String beforeValue;
    private final String afterValue;
    private final Long undoNextLsn;

    public LogRecord(long lsn, long transactionId, Type type, String key, String beforeValue, String afterValue) {
        this(lsn, transactionId, type, key, beforeValue, afterValue, null);
    }

    public LogRecord(long lsn, long transactionId, Type type, String key, String beforeValue, String afterValue, Long undoNextLsn) {
        this.lsn = lsn;
        this.transactionId = transactionId;
        this.type = type;
        this.key = key;
        this.beforeValue = beforeValue;
        this.afterValue = afterValue;
        this.undoNextLsn = undoNextLsn;
    }

    public long getLsn() {
        return lsn;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getBeforeValue() {
        return beforeValue;
    }

    public String getAfterValue() {
        return afterValue;
    }

    public Long getUndoNextLsn() {
        return undoNextLsn;
    }
}
