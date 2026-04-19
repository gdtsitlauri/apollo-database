package com.apollo.query;

public class CostModel {
    public double estimateSequentialScanCost(Statistics statistics) {
        return statistics.rowCount();
    }

    public double estimateIndexScanCost(Statistics statistics, double selectivity) {
        return Math.max(1.0, statistics.rowCount() * selectivity * 0.25 + 8.0);
    }

    public double estimateJoinCost(long leftRows, long rightRows) {
        return leftRows * rightRows;
    }

    public double estimateHashJoinCost(long leftRows, long rightRows) {
        return leftRows + rightRows + (leftRows * 0.15);
    }

    public double estimateMergeJoinCost(long leftRows, long rightRows) {
        return leftRows + rightRows + ((leftRows + rightRows) * 0.25);
    }
}
