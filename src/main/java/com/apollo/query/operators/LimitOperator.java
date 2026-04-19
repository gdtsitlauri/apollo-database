package com.apollo.query.operators;

import java.util.Map;

public class LimitOperator implements Operator {
    private final Operator child;
    private final int limit;
    private int emitted;

    public LimitOperator(Operator child, int limit) {
        this.child = child;
        this.limit = limit;
    }

    @Override
    public void open() {
        emitted = 0;
        child.open();
    }

    @Override
    public Map<String, String> next() {
        if (emitted >= limit) {
            return null;
        }
        Map<String, String> row = child.next();
        if (row != null) {
            emitted++;
        }
        return row;
    }

    @Override
    public void close() {
        child.close();
    }
}
