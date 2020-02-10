/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.prestosql.sql.planner.assertions;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;

/**
 * 实验用来验证逻辑查询计划的框架：PlanMatchPattern
 */
public class TestPlanMatchPattern
        extends BasePlanTest
{
    @Test
    public void testTableScan()
    {
        /**
         * sqlParser.createStatement(sql) -> Statement statement
         * 以查询"select nationkey from nation"为例，statement的结构为：
         * Statement -> Query (impl)
         *   QueryBody -> QuerySpecification (impl)
         *     select=Select{distinct=false, selectItems=[nationkey]},
         *     from=Optional[Table{nation}]
         *
         * LogicalPlanner.plan(Analysis analysis)，其中analysis里面Statement root就是如上面的结构：
         * Query{queryBody=QuerySpecification{select=Select{distinct=false, selectItems=[nationkey]}, from=Optional[Table{nation}], ...}
         */
//        assertDistributedPlan("select nationkey from nation",
//                anyTree(
//                        tableScan("nation")));

        /**
         * 如果只是select，不包含条件"where regionkey = 1"，则这个测试会出错
         *
         * LogicalPlanner.plan() -> PlanNode root
         * OutputNode
         *   ...
         *   source = ProjectNode
         *     source = FilterNode
         *       source = TableScanNode
         *         table=local:nation:sf0.01
         *         outputSymbols=[nationkey, name, regionkey, comment, row_number]
         *         assignments={nationkey=tpch:nationkey, name=tpch:name, regionkey=tpch:regionkey, comment=tpch:comment, row_number=tpch:row_number}
         *         enforcedConstraint=TupleDomain{ALL}}
         *       predicate = ("regionkey" = CAST(1 AS bigint))
         *       id = "1"
         *     assignments:
         *       "nationkey_0" -> "nationkey"
         *     id = "2"
         *   上面还有几层ProjectNode，分别映射："nationkey_1" -> "nationkey_0", "expr" -> "nationkey_1", "expr_2" -> "expr"，并且id的值递增
         * columnNames:
         *   0 = "nationkey"
         * outputs:
         *   0 = "expr_2"
         * id = "6"
         *
         */
        assertDistributedPlan("select nationkey from nation where regionkey = 1",
                anyTree(
                        tableScan("nation", ImmutableMap.of("regionkey", "regionkey"))));
    }
}
