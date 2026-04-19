package com.apollo.query;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLParser {
    private static final Pattern CREATE_TABLE = Pattern.compile("(?i)^create\\s+table\\s+(\\w+)\\s*\\(([^)]+)\\)$");
    private static final Pattern INSERT = Pattern.compile("(?i)^insert\\s+into\\s+(\\w+)\\s+values\\s*\\(([^)]+)\\)$");
    private static final Pattern UPDATE = Pattern.compile("(?i)^update\\s+(\\w+)\\s+set\\s+(.+?)(?:\\s+where\\s+(.+))?$");
    private static final Pattern DELETE = Pattern.compile("(?i)^delete\\s+from\\s+(\\w+)(?:\\s+where\\s+(.+))?$");

    public AST parse(String sql) {
        String normalized = normalize(sql);
        String statementType = normalized.split("\\s+")[0].toUpperCase(Locale.ROOT);
        return switch (statementType) {
            case "SELECT" -> parseSelect(normalized);
            case "INSERT" -> parseInsert(normalized);
            case "UPDATE" -> parseUpdate(normalized);
            case "DELETE" -> parseDelete(normalized);
            case "CREATE" -> parseCreateTable(normalized);
            default -> throw new IllegalArgumentException("Unsupported SQL: " + sql);
        };
    }

    private AST parseSelect(String sql) {
        String body = sql.substring("SELECT ".length());
        int fromIndex = indexOfClause(body, " FROM ");
        String selectPart = body.substring(0, fromIndex).trim();
        String remainder = body.substring(fromIndex + 6).trim();

        Integer limit = null;
        int limitIndex = indexOfClause(remainder, " LIMIT ");
        if (limitIndex >= 0) {
            limit = Integer.parseInt(remainder.substring(limitIndex + 7).trim());
            remainder = remainder.substring(0, limitIndex).trim();
        }

        AST.OrderBy orderBy = null;
        int orderByIndex = indexOfClause(remainder, " ORDER BY ");
        if (orderByIndex >= 0) {
            orderBy = parseOrderBy(remainder.substring(orderByIndex + 10).trim());
            remainder = remainder.substring(0, orderByIndex).trim();
        }

        String groupBy = null;
        int groupIndex = indexOfClause(remainder, " GROUP BY ");
        if (groupIndex >= 0) {
            groupBy = remainder.substring(groupIndex + 10).trim();
            remainder = remainder.substring(0, groupIndex).trim();
        }

        AST.Predicate predicate = null;
        int whereIndex = indexOfClause(remainder, " WHERE ");
        if (whereIndex >= 0) {
            predicate = parsePredicate(remainder.substring(whereIndex + 7).trim());
            remainder = remainder.substring(0, whereIndex).trim();
        }

        AST.JoinClause joinClause = null;
        String tableName;
        int joinIndex = indexOfClause(remainder, " JOIN ");
        if (joinIndex >= 0) {
            tableName = remainder.substring(0, joinIndex).trim();
            String joinRemainder = remainder.substring(joinIndex + 6).trim();
            int onIndex = indexOfClause(joinRemainder, " ON ");
            String joinTable = joinRemainder.substring(0, onIndex).trim();
            String[] joinParts = joinRemainder.substring(onIndex + 4).trim().split("=");
            joinClause = new AST.JoinClause(joinTable, joinParts[0].trim(), joinParts[1].trim());
        } else {
            tableName = remainder.trim();
        }

        AST.Aggregate aggregate = parseAggregate(selectPart, groupBy);
        List<String> columns = splitCsv(selectPart);
        return new AST(AST.StatementType.SELECT, sql, tableName, columns, List.of(), List.of(), Map.of(), predicate, limit, orderBy, joinClause, aggregate);
    }

    private AST parseInsert(String sql) {
        Matcher matcher = INSERT.matcher(sql);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid INSERT: " + sql);
        }
        return new AST(AST.StatementType.INSERT, sql, matcher.group(1), List.of(), splitCsv(matcher.group(2)), List.of(), Map.of(), null, null, null, null, null);
    }

    private AST parseUpdate(String sql) {
        Matcher matcher = UPDATE.matcher(sql);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid UPDATE: " + sql);
        }

        Map<String, String> assignments = new LinkedHashMap<>();
        for (String assignment : splitCsv(matcher.group(2))) {
            String[] parts = assignment.split("=");
            assignments.put(parts[0].trim(), unquote(parts[1].trim()));
        }

        AST.Predicate predicate = matcher.group(3) == null ? null : parsePredicate(matcher.group(3));
        return new AST(AST.StatementType.UPDATE, sql, matcher.group(1), List.of(), List.of(), List.of(), assignments, predicate, null, null, null, null);
    }

    private AST parseDelete(String sql) {
        Matcher matcher = DELETE.matcher(sql);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid DELETE: " + sql);
        }
        AST.Predicate predicate = matcher.group(2) == null ? null : parsePredicate(matcher.group(2));
        return new AST(AST.StatementType.DELETE, sql, matcher.group(1), List.of(), List.of(), List.of(), Map.of(), predicate, null, null, null, null);
    }

    private AST parseCreateTable(String sql) {
        Matcher matcher = CREATE_TABLE.matcher(sql);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid CREATE TABLE: " + sql);
        }
        return new AST(AST.StatementType.CREATE_TABLE, sql, matcher.group(1), List.of(), List.of(), splitCsv(matcher.group(2)), Map.of(), null, null, null, null, null);
    }

    private AST.Predicate parsePredicate(String expression) {
        String[] parts = expression.split("=");
        return new AST.Predicate(parts[0].trim(), unquote(parts[1].trim()));
    }

    private AST.Aggregate parseAggregate(String selectPart, String groupBy) {
        String upper = selectPart.toUpperCase(Locale.ROOT);
        if (!upper.contains("COUNT(") && !upper.contains("SUM(")) {
            return null;
        }

        String function = upper.contains("COUNT(") ? "COUNT" : "SUM";
        int aggregateStart = upper.indexOf(function + "(") + function.length() + 1;
        int aggregateEnd = upper.indexOf(')', aggregateStart);
        String column = selectPart.substring(aggregateStart, aggregateEnd).trim();
        return new AST.Aggregate(function, column, groupBy);
    }

    private AST.OrderBy parseOrderBy(String clause) {
        String[] parts = clause.trim().split("\\s+");
        boolean ascending = parts.length < 2 || !parts[1].equalsIgnoreCase("DESC");
        return new AST.OrderBy(parts[0], ascending);
    }

    private List<String> splitCsv(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .map(this::unquote)
                .toList();
    }

    private String normalize(String sql) {
        return sql.trim().replaceAll("\\s+", " ");
    }

    private String unquote(String value) {
        if ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private int indexOfClause(String value, String clause) {
        return value.toUpperCase(Locale.ROOT).indexOf(clause.toUpperCase(Locale.ROOT));
    }
}
