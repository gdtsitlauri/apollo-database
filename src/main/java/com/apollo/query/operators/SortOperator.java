package com.apollo.query.operators;

import com.apollo.query.AST;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class SortOperator implements Operator {
    private final Operator child;
    private final AST.OrderBy orderBy;
    private List<Map<String, String>> rows;
    private int index;

    public SortOperator(Operator child, AST.OrderBy orderBy) {
        this.child = child;
        this.orderBy = orderBy;
    }

    @Override
    public void open() {
        child.open();
        rows = new ArrayList<>();
        try {
            Map<String, String> row;
            while ((row = child.next()) != null) {
                rows.add(row);
            }
        } finally {
            child.close();
        }
        Comparator<Map<String, String>> comparator = Comparator.comparing(
                row -> RowUtils.valueForColumn(row, orderBy.column()),
                Comparator.nullsFirst(String::compareTo));
        if (!orderBy.ascending()) {
            comparator = comparator.reversed();
        }
        rows.sort(comparator);
        index = 0;
    }

    @Override
    public Map<String, String> next() {
        return rows != null && index < rows.size() ? rows.get(index++) : null;
    }

    @Override
    public void close() {
        rows = null;
        index = 0;
    }
}
