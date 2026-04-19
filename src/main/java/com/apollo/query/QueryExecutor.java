package com.apollo.query;

import com.apollo.query.operators.AggregateOperator;
import com.apollo.query.operators.FilterOperator;
import com.apollo.query.operators.HashJoinOperator;
import com.apollo.query.operators.IndexScanOperator;
import com.apollo.query.operators.LimitOperator;
import com.apollo.query.operators.MergeJoinOperator;
import com.apollo.query.operators.NestedLoopJoinOperator;
import com.apollo.query.operators.Operator;
import com.apollo.query.operators.ProjectOperator;
import com.apollo.query.operators.SeqScanOperator;
import com.apollo.query.operators.SortOperator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class QueryExecutor {
    private final DatabaseCatalog catalog;

    public QueryExecutor(DatabaseCatalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    public QueryResult execute(PhysicalPlan plan) {
        return execute(plan, null);
    }

    public QueryResult execute(PhysicalPlan plan, QuerySession session) {
        AST ast = plan.ast();
        return switch (ast.statementType()) {
            case CREATE_TABLE -> executeCreate(ast);
            case INSERT -> executeInsert(ast, session);
            case UPDATE -> executeUpdate(ast, session);
            case DELETE -> executeDelete(ast, session);
            case SELECT -> executeSelect(plan, session);
        };
    }

    private QueryResult executeCreate(AST ast) {
        catalog.createTable(ast.tableName(), ast.tableDefinition());
        return QueryResult.message("Created table " + ast.tableName());
    }

    private QueryResult executeInsert(AST ast, QuerySession session) {
        DatabaseCatalog.Table table = catalog.table(ast.tableName());
        if (table.columns().size() != ast.values().size()) {
            throw new IllegalArgumentException("Value count does not match table definition");
        }
        Map<String, String> row = new LinkedHashMap<>();
        for (int i = 0; i < table.columns().size(); i++) {
            row.put(table.columns().get(i), ast.values().get(i));
        }
        table.insertRow(row, session);
        return QueryResult.message("Inserted 1 row into " + ast.tableName());
    }

    private QueryResult executeUpdate(AST ast, QuerySession session) {
        DatabaseCatalog.Table table = catalog.table(ast.tableName());
        int updated = table.updateRows(ast.predicate(), ast.assignments(), session);
        return QueryResult.message("Updated " + updated + " rows");
    }

    private QueryResult executeDelete(AST ast, QuerySession session) {
        DatabaseCatalog.Table table = catalog.table(ast.tableName());
        int deleted = table.deleteRows(ast.predicate(), session);
        return QueryResult.message("Deleted " + deleted + " rows");
    }

    private QueryResult executeSelect(PhysicalPlan plan, QuerySession session) {
        Operator root = buildOperatorPipeline(plan, session);
        List<Map<String, String>> rows = materialize(root);
        return QueryResult.rows(rows);
    }

    private Operator buildOperatorPipeline(PhysicalPlan plan, QuerySession session) {
        AST ast = plan.ast();
        Operator operator = ast.joinClause() == null ? buildScan(ast, plan, session) : buildJoin(ast, plan, session);

        if (ast.predicate() != null && requiresFilter(ast, plan)) {
            operator = new FilterOperator(operator, ast.predicate());
        }
        if (ast.aggregate() != null) {
            operator = new AggregateOperator(operator, ast.aggregate());
        }
        if (ast.orderBy() != null) {
            operator = new SortOperator(operator, ast.orderBy());
        }
        if (!(ast.columns().size() == 1 && ast.columns().get(0).equals("*")) || ast.aggregate() != null) {
            operator = new ProjectOperator(operator, ast.columns());
        }
        if (ast.limit() != null) {
            operator = new LimitOperator(operator, ast.limit());
        }
        return operator;
    }

    private Operator buildScan(AST ast, PhysicalPlan plan, QuerySession session) {
        DatabaseCatalog.Table table = catalog.table(ast.tableName());
        if ("INDEX_SCAN".equals(plan.operators().get(0)) && ast.predicate() != null && ast.predicate().column().equals(table.indexedColumn())) {
            return new IndexScanOperator(table.lookupEquals(ast.predicate().column(), ast.predicate().value(), session));
        }
        return new SeqScanOperator(table.scanRows(session));
    }

    private Operator buildJoin(AST ast, PhysicalPlan plan, QuerySession session) {
        DatabaseCatalog.Table left = catalog.table(ast.tableName());
        DatabaseCatalog.Table right = catalog.table(ast.joinClause().tableName());
        Operator leftSource = new SeqScanOperator(left.scanRows(session));
        Operator rightSource = new SeqScanOperator(right.scanRows(session));
        return switch (plan.operators().get(0)) {
            case "HASH_JOIN" -> new HashJoinOperator(
                    leftSource,
                    rightSource,
                    ast.tableName(),
                    ast.joinClause().tableName(),
                    ast.joinClause().leftColumn(),
                    ast.joinClause().rightColumn());
            case "MERGE_JOIN" -> new MergeJoinOperator(
                    leftSource,
                    rightSource,
                    ast.tableName(),
                    ast.joinClause().tableName(),
                    ast.joinClause().leftColumn(),
                    ast.joinClause().rightColumn());
            default -> new NestedLoopJoinOperator(
                    leftSource,
                    rightSource,
                    ast.tableName(),
                    ast.joinClause().tableName(),
                    ast.joinClause().leftColumn(),
                    ast.joinClause().rightColumn());
        };
    }

    private boolean requiresFilter(AST ast, PhysicalPlan plan) {
        return ast.joinClause() != null || !"INDEX_SCAN".equals(plan.operators().get(0));
    }

    private List<Map<String, String>> materialize(Operator operator) {
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
