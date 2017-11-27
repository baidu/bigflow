/***************************************************************************
 *
 * Copyright (c) 2014 Baidu, Inc. All Rights Reserved.
 *
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
 *
 **************************************************************************/
// Author: Wang Cong <wangcong09@baidu.com>

#include "gtest/gtest.h"

#include "flume/planner/common/check_cycle_pass.h"
#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

TEST(CheckCyclePassTest, EverythingIsOK) {
    Plan plan;
    Unit* n1 = plan.NewUnit("n1", true);
    Unit* n2 = plan.NewUnit("n2", true);
    Unit* n3 = plan.NewUnit("n3", true);
    Unit* n4 = plan.NewUnit("n4", true);
    Unit* n5 = plan.NewUnit("n5", true);

    plan.AddDependency(n1, n2);
    plan.AddDependency(n2, n3);
    plan.AddDependency(n1, n4);
    plan.AddDependency(n3, n5);
    plan.AddDependency(n4, n5);

    Unit* s1 = plan.NewUnit("s1", false);
    Unit* s2 = plan.NewUnit("s2", false);
    Unit* s3 = plan.NewUnit("s3", false);

    Unit* root = plan.Root();
    plan.AddControl(root, s1);
    plan.AddControl(s1, n1);
    plan.AddControl(s1, n2);
    plan.AddControl(root, s2);
    plan.AddControl(s2, n3);
    plan.AddControl(s2, n4);
    plan.AddControl(root, s3);
    plan.AddControl(s3, n5);

    EXPECT_FALSE(CheckCyclePass().Run(&plan));
}

TEST(CheckCyclePassDeathTest, FindCycle) {
    Plan plan;
    Unit* n_1_1 = plan.NewUnit("n_1_1", true);
    Unit* n_1_2 = plan.NewUnit("n_1_2", true);
    Unit* n_0_1 = plan.NewUnit("n_0_1", true);
    Unit* n_1_3 = plan.NewUnit("n_1_3", true);

    plan.AddDependency(n_1_1, n_1_2);
    plan.AddDependency(n_1_2, n_0_1);
    plan.AddDependency(n_0_1, n_1_3);

    Unit* s1 = plan.NewUnit("s1", false);
    PbScope pb_scope;
    s1->set<PbScope>(pb_scope);

    Unit* root = plan.Root();
    plan.AddControl(root, s1);
    plan.AddControl(s1, n_1_1);
    plan.AddControl(s1, n_1_2);
    plan.AddControl(s1, n_1_3);

    // Now we made a cycle for s1 like this:
    //
    //   +-------+
    //   | n_0_1 |----------------+
    //   |       |                |
    //   +-------+                |
    //       ^                    |
    // +-----|--------------------|-----+
    // |     |                   \/     |
    // | +-------+            +-------+ |
    // | | n_1_2 |            | n_1_3 | |
    // | |       |            |       | |
    // | +-------+            +-------+ |
    // |     ^                          |
    // |     |                          |
    // | +-------+     Scope s1         |
    // | | n_1_1 |                      |
    // | |       |                      |
    // | +-------+                      |
    // +--------------------------------+
    //
    // And a failed check is expected.

    EXPECT_FALSE(DataFlowAnalysis().Run(&plan));
    EXPECT_DEATH(CheckCyclePass().Run(&plan), "Scope cycle is found");
}

} // namespace planner
} // namespace flume
} // namespace baidu

