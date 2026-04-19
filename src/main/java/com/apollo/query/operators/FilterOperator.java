package com.apollo.query.operators;

import com.apollo.query.AST;

import java.util.Map;
import java.util.Objects;

public class FilterOperator implements Operator {
    private final Operator child;
    private final AST.Predicate predicate;

    public FilterOperator(Operator child, AST.Predicate predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override
    public void open() {
        child.open();
    }

    @Override
    public Map<String, String> next() {
        Map<String, String> row;
        while ((row = child.next()) != null) {
            if (predicate == null || Objects.equals(RowUtils.valueForColumn(row, predicate.column()), predicate.value())) {
                return row;
            }
        }
        return null;
    }

    @Override
    public void close() {
        child.close();
    }
}
