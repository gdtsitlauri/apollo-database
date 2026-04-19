package com.apollo.query.operators;

import java.util.List;
import java.util.Map;

public class IndexScanOperator extends SeqScanOperator {
    public IndexScanOperator(List<Map<String, String>> rows) {
        super(rows);
    }
}
