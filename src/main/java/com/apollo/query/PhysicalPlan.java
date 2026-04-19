package com.apollo.query;

import java.util.List;

public record PhysicalPlan(String rootOperator, List<String> operators, AST ast) {
}
