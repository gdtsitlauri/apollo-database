package com.apollo.query.operators;

import java.util.Map;

public interface Operator {
    void open();

    Map<String, String> next();

    void close();
}
