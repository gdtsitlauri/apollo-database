package com.apollo.query.operators;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HashJoinOperator extends NestedLoopJoinOperator {
    private final Operator left;
    private final Operator right;
    private final String leftTable;
    private final String rightTable;
    private final String leftColumn;
    private final String rightColumn;
    private List<Map<String, String>> joinedRows;
    private int index;

    public HashJoinOperator(
            Operator left,
            Operator right,
            String leftTable,
            String rightTable,
            String leftColumn,
            String rightColumn
    ) {
        super(left, right, leftTable, rightTable, leftColumn, rightColumn);
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
        Map<String, List<Map<String, String>>> hash = new LinkedHashMap<>();
        for (Map<String, String> rightRow : rightRows) {
            hash.computeIfAbsent(RowUtils.lookupQualified(rightRow, rightColumn), ignored -> new ArrayList<>()).add(rightRow);
        }
        joinedRows = new ArrayList<>();
        for (Map<String, String> leftRow : leftRows) {
            for (Map<String, String> rightRow : hash.getOrDefault(RowUtils.lookupQualified(leftRow, leftColumn), List.of())) {
                Map<String, String> row = new LinkedHashMap<>();
                row.putAll(RowUtils.qualified(leftTable, leftRow));
                row.putAll(RowUtils.qualified(rightTable, rightRow));
                joinedRows.add(row);
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
}
