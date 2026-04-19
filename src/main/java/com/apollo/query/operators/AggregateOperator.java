package com.apollo.query.operators;

import com.apollo.query.AST;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AggregateOperator implements Operator {
    private final Operator child;
    private final AST.Aggregate aggregate;
    private List<Map<String, String>> rows;
    private int index;

    public AggregateOperator(Operator child, AST.Aggregate aggregate) {
        this.child = child;
        this.aggregate = aggregate;
    }

    @Override
    public void open() {
        child.open();
        Map<String, Long> values = new LinkedHashMap<>();
        Map<String, String> groupLabels = new LinkedHashMap<>();
        try {
            Map<String, String> row;
            while ((row = child.next()) != null) {
                String group = aggregate.groupByColumn() == null ? "ALL" : RowUtils.valueForColumn(row, aggregate.groupByColumn());
                groupLabels.put(group, group);
                if (aggregate.function().equals("COUNT")) {
                    values.merge(group, 1L, Long::sum);
                } else if (aggregate.function().equals("SUM")) {
                    long number = Long.parseLong(RowUtils.valueForColumn(row, aggregate.column()));
                    values.merge(group, number, Long::sum);
                }
            }
        } finally {
            child.close();
        }
        rows = new ArrayList<>();
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            Map<String, String> row = new LinkedHashMap<>();
            if (aggregate.groupByColumn() != null) {
                row.put(aggregate.groupByColumn(), groupLabels.get(entry.getKey()));
            }
            String label = aggregate.function() + "(" + aggregate.column() + ")";
            row.put(label, Long.toString(entry.getValue()));
            if (aggregate.function().equals("COUNT") && aggregate.column().equals("*")) {
                row.put("COUNT(*)", Long.toString(entry.getValue()));
            }
            rows.add(row);
        }
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
