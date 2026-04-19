package com.apollo.query;

import java.util.List;
import java.util.Map;

public record AST(
        StatementType statementType,
        String sql,
        String tableName,
        List<String> columns,
        List<String> values,
        List<String> tableDefinition,
        Map<String, String> assignments,
        Predicate predicate,
        Integer limit,
        OrderBy orderBy,
        JoinClause joinClause,
        Aggregate aggregate
) {
    public enum StatementType {
        SELECT,
        INSERT,
        UPDATE,
        DELETE,
        CREATE_TABLE
    }

    public record Predicate(String column, String value) {
    }

    public record JoinClause(String tableName, String leftColumn, String rightColumn) {
    }

    public record OrderBy(String column, boolean ascending) {
    }

    public record Aggregate(String function, String column, String groupByColumn) {
    }
}
