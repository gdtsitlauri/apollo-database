package com.apollo.query.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class NestedLoopJoinOperator implements Operator {
    private final Operator left;
    private final Operator right;
    private final String leftTable;
    private final String rightTable;
    private final String leftColumn;
    private final String rightColumn;
    private List<Map<String, String>> joinedRows;
    private int index;

    public NestedLoopJoinOperator(
            Operator left,
            Operator right,
            String leftTable,
            String rightTable,
            String leftColumn,
            String rightColumn
    ) {
        this.left = left;
        this.right = right;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

    @Override
    public void open() {
        List<Map<String, String>> leftRows = materialize(left);
        List<Map<String, String>> rightRows = materialize(right);
        joinedRows = new ArrayList<>();
        for (Map<String, String> leftRow : leftRows) {
            for (Map<String, String> rightRow : rightRows) {
                if (Objects.equals(RowUtils.lookupQualified(leftRow, leftColumn), RowUtils.lookupQualified(rightRow, rightColumn))) {
                    Map<String, String> row = new java.util.LinkedHashMap<>();
                    row.putAll(RowUtils.qualified(leftTable, leftRow));
                    row.putAll(RowUtils.qualified(rightTable, rightRow));
                    joinedRows.add(row);
                }
            }
        }
        index = 0;
    }

    @Override
    public Map<String, String> next() {
        return joinedRows != null && index < joinedRows.size() ? joinedRows.get(index++) : null;
    }

    @Override
    public void close() {
        joinedRows = null;
        index = 0;
    }

    protected List<Map<String, String>> materialize(Operator operator) {
        List<Map<String, String>> rows = new ArrayList<>();
        operator.open();
        try {
            Map<String, String> row;
            while ((row = operator.next()) != null) {
                rows.add(row);
            }
            return rows;
        } finally {
            operator.close();
        }
    }
}
