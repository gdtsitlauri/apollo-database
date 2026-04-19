package com.apollo.query;

public record Statistics(long rowCount, long columnCount) {
    public Statistics(long rowCount) {
        this(rowCount, 0);
    }
}
