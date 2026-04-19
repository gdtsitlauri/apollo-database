package com.apollo.query.operators;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ProjectOperator implements Operator {
    private final Operator child;
    private final List<String> columns;

    public ProjectOperator(Operator child, List<String> columns) {
        this.child = child;
        this.columns = columns;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Map<String, String> next() {
        Map<String, String> row = child.next();
        if (row == null) {
            return null;
        }
        if (columns.size() == 1 && columns.get(0).equals("*")) {
            return row;
        }
        Map<String, String> projection = new LinkedHashMap<>();
        for (String column : columns) {
            projection.put(column, RowUtils.valueForColumn(row, column));
        }
        return projection;
    }

    @Override
    public void close() {
        child.close();
    }
}
