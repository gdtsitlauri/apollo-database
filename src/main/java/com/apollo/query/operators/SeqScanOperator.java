package com.apollo.query.operators;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SeqScanOperator implements Operator {
    private final List<Map<String, String>> rows;
    private Iterator<Map<String, String>> iterator;

    public SeqScanOperator(List<Map<String, String>> rows) {
        this.rows = rows;
    }

    @Override
    public void open() {
        iterator = rows.iterator();
    }

    @Override
    public Map<String, String> next() {
        return iterator != null && iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public void close() {
        iterator = null;
    }
}
