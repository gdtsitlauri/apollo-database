package com.apollo.query.operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MergeJoinOperator extends NestedLoopJoinOperator {
    private final Operator left;
    private final Operator right;
    private final String leftTable;
    private final String rightTable;
    private final String leftColumn;
    private final String rightColumn;
    private List<Map<String, String>> joinedRows;
    private int index;

    public MergeJoinOperator(
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
        List<Map<String, String>> leftRows = new ArrayList<>(materialize(left));
        List<Map<String, String>> rightRows = new ArrayList<>(materialize(right));
        leftRows.sort(Comparator.comparing(row -> RowUtils.lookupQualified(row, leftColumn)));
        rightRows.sort(Comparator.comparing(row -> RowUtils.lookupQualified(row, rightColumn)));

        joinedRows = new ArrayList<>();
        int i = 0;
        int j = 0;
        while (i < leftRows.size() && j < rightRows.size()) {
            String leftKey = RowUtils.lookupQualified(leftRows.get(i), leftColumn);
            String rightKey = RowUtils.lookupQualified(rightRows.get(j), rightColumn);
            int cmp = leftKey.compareTo(rightKey);
            if (cmp < 0) {
                i++;
            } else if (cmp > 0) {
                j++;
            } else {
                List<Map<String, String>> leftGroup = new ArrayList<>();
                List<Map<String, String>> rightGroup = new ArrayList<>();
                while (i < leftRows.size() && RowUtils.lookupQualified(leftRows.get(i), leftColumn).equals(leftKey)) {
                    leftGroup.add(leftRows.get(i++));
                }
                while (j < rightRows.size() && RowUtils.lookupQualified(rightRows.get(j), rightColumn).equals(rightKey)) {
                    rightGroup.add(rightRows.get(j++));
                }
                for (Map<String, String> leftRow : leftGroup) {
                    for (Map<String, String> rightRow : rightGroup) {
                        Map<String, String> row = new LinkedHashMap<>();
                        row.putAll(RowUtils.qualified(leftTable, leftRow));
                        row.putAll(RowUtils.qualified(rightTable, rightRow));
                        joinedRows.add(row);
                    }
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
}
