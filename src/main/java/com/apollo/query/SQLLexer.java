package com.apollo.query;

import java.util.Arrays;
import java.util.List;

public class SQLLexer {
    public List<String> tokenize(String sql) {
        return Arrays.asList(sql.trim().split("\\s+"));
    }
}
