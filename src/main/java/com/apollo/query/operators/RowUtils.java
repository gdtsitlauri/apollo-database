package com.apollo.query.operators;

import java.util.LinkedHashMap;
import java.util.Map;

final class RowUtils {
    private RowUtils() {
    }

    static String valueForColumn(Map<String, String> row, String column) {
        if (row.containsKey(column)) {
            return row.get(column);
        }
        int dot = column.indexOf('.');
        if (dot >= 0) {
            String unqualified = column.substring(dot + 1);
            if (row.containsKey(unqualified)) {
                return row.get(unqualified);
            }
            return row.get(column);
        }
        return row.entrySet().stream()
                .filter(entry -> entry.getKey().endsWith("." + column))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(row.get(column));
    }

    static String lookupQualified(Map<String, String> row, String column) {
        int dot = column.indexOf('.');
        String unqualified = dot >= 0 ? column.substring(dot + 1) : column;
        return row.get(unqualified);
    }

    static Map<String, String> qualified(String tableName, Map<String, String> row) {
        Map<String, String> qualified = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : row.entrySet()) {
            qualified.put(tableName + "." + entry.getKey(), entry.getValue());
        }
        return qualified;
    }
}
