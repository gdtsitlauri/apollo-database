package com.apollo.query;

import java.util.List;

public record LogicalPlan(String rootOperator, List<String> steps) {
}
