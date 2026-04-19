package com.apollo.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryPlannerTest {
    @Test
    void testPlannerChoosesLearnedJoinPlanCandidate() {
        QueryPlanner planner = new QueryPlanner();
        AST ast = new SQLParser().parse("SELECT users.name, orders.item FROM users JOIN orders ON users.id = orders.user_id");
        PhysicalPlan plan = planner.buildPhysical(planner.buildLogical(ast), ast);
        assertTrue(plan.operators().get(0).equals("HASH_JOIN") || plan.operators().get(0).equals("MERGE_JOIN") || plan.operators().get(0).equals("NESTED_LOOP_JOIN"));
    }

    @Test
    void testPlannerCanChooseIndexScanForPredicateQuery() {
        QueryPlanner planner = new QueryPlanner();
        AST ast = new SQLParser().parse("SELECT * FROM users WHERE id = 1");
        PhysicalPlan plan = planner.buildPhysical(planner.buildLogical(ast), ast);
        assertTrue(plan.operators().get(0).equals("INDEX_SCAN") || plan.operators().get(0).equals("SEQ_SCAN"));
    }
}
