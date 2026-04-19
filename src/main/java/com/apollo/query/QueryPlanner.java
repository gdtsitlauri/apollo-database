package com.apollo.query;

import com.apollo.lens.LensModel;

import java.util.ArrayList;
import java.util.List;

public class QueryPlanner {
    private final LensModel lensModel;
    private final CostModel costModel;

    public QueryPlanner() {
        this(new LensModel(), new CostModel());
    }

    public QueryPlanner(LensModel lensModel, CostModel costModel) {
        this.lensModel = lensModel;
        this.costModel = costModel;
    }

    public LogicalPlan buildLogical(AST ast) {
        List<String> steps = new ArrayList<>();
        steps.add("SCAN " + ast.tableName());
        if (ast.joinClause() != null) {
            steps.add("JOIN " + ast.joinClause().tableName());
        }
        if (ast.predicate() != null) {
            steps.add("FILTER " + ast.predicate().column());
        }
        if (ast.aggregate() != null) {
            steps.add("AGGREGATE " + ast.aggregate().function());
        }
        if (ast.orderBy() != null) {
            steps.add("SORT " + ast.orderBy().column());
        }
        if (!ast.assignments().isEmpty()) {
            steps.add("UPDATE SET");
        }
        return new LogicalPlan(ast.statementType().name(), steps);
    }

    public PhysicalPlan buildPhysical(LogicalPlan logicalPlan, AST ast) {
        List<String> operators = new ArrayList<>();
        operators.add(selectPrimaryOperator(ast));
        if (ast.predicate() != null) {
            operators.add("FILTER");
        }
        if (ast.aggregate() != null) {
            operators.add("HASH_AGGREGATE");
        }
        if (ast.orderBy() != null) {
            operators.add("SORT");
        }
        if (ast.limit() != null) {
            operators.add("LIMIT");
        }
        return new PhysicalPlan(logicalPlan.rootOperator(), operators, ast);
    }

    private String selectPrimaryOperator(AST ast) {
        return switch (ast.statementType()) {
            case CREATE_TABLE -> "DDL_CREATE";
            case INSERT -> "TABLE_INSERT";
            case UPDATE -> "TABLE_UPDATE";
            case DELETE -> "TABLE_DELETE";
            case SELECT -> {
                if (ast.joinClause() != null) {
                    yield lensModel.choosePlan(List.of(
                            new LensModel.PlanCandidate("NESTED_LOOP_JOIN", costModel.estimateJoinCost(256, 128), 0.05),
                            new LensModel.PlanCandidate("HASH_JOIN", costModel.estimateHashJoinCost(256, 128), 0.12),
                            new LensModel.PlanCandidate("MERGE_JOIN", costModel.estimateMergeJoinCost(256, 128), 0.08)
                    ));
                }
                yield lensModel.choosePlan(List.of(
                        new LensModel.PlanCandidate("SEQ_SCAN", costModel.estimateSequentialScanCost(new Statistics(1000)), 0.04),
                        new LensModel.PlanCandidate("INDEX_SCAN", costModel.estimateIndexScanCost(new Statistics(1000), ast.predicate() != null ? 0.05 : 0.5), 0.1)
                ));
            }
        };
    }
}
