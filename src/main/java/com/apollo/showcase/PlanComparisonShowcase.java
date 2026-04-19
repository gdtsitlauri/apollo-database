package com.apollo.showcase;

import com.apollo.query.AST;
import com.apollo.query.CostModel;
import com.apollo.query.LogicalPlan;
import com.apollo.query.PhysicalPlan;
import com.apollo.query.QueryPlanner;
import com.apollo.query.SQLParser;

public class PlanComparisonShowcase {
    public String compare(String sql) {
        SQLParser parser = new SQLParser();
        QueryPlanner planner = new QueryPlanner();
        AST ast = parser.parse(sql);
        LogicalPlan logical = planner.buildLogical(ast);
        PhysicalPlan physical = planner.buildPhysical(logical, ast);
        CostModel costModel = new CostModel();
        return "Logical=" + logical.steps() + ", Physical=" + physical.operators()
                + ", SeqCost=" + costModel.estimateSequentialScanCost(new com.apollo.query.Statistics(1000));
    }
}
