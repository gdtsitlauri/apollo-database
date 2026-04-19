package com.apollo.query;

import java.util.List;
import java.util.Map;

public record QueryResult(List<Map<String, String>> rows, String message) {
    public static QueryResult rows(List<Map<String, String>> rows) {
        return new QueryResult(rows, null);
    }

    public static QueryResult message(String message) {
        return new QueryResult(List.of(), message);
    }
}
