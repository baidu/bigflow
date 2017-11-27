/***************************************************************************
 *
 * Copyright (c) 2013 Baidu, Inc. All Rights Reserved.
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
// Author: Wang Song <wangsong06@baidu.com>
//

#include "flume/planner/monitor/prepared_node_analysis.h"

#include "gtest/gtest.h"

#include "flume/planner/plan.h"
#include "flume/planner/testing/plan_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

class PreparedNodeAnalysisTest : public PlanTest {};

TEST_F(PreparedNodeAnalysisTest, Run) {
    AddTasksOf(1);
    Unit* n11 = AddLeafBelowTask(0, "n11", Unit::LOAD_NODE);
    Unit* n12 = AddLeafBelowTask(0, "n12", Unit::SHUFFLE_NODE);
    Unit* n13 = AddLeafBelowTask(0, "n13", Unit::PROCESS_NODE);
    Unit* n14 = AddLeafBelowTask(0, "n14", Unit::PROCESS_NODE);
    Unit* n15 = AddLeafBelowTask(0, "n15", Unit::PROCESS_NODE);
    Unit* n16 = AddLeafBelowTask(0, "n16", Unit::PROCESS_NODE);
    Unit* n17 = AddLeafBelowTask(0, "n17", Unit::PROCESS_NODE);

    SetProcess(n13);
    SetProcess(n14);
    SetProcess(n15);
    SetProcess(n16);
    SetProcess(n17);

    AddInput(n11, n12, n11->identity(), false);
    AddInput(n12, n14, n12->identity(), false);
    AddInput(n13, n14, n13->identity(), false);
    AddInput(n14, n15, n14->identity(), false);
    AddInput(n15, n17, n15->identity(), false);
    AddInput(n16, n17, n16->identity(), false);

    n17->get<PbLogicalPlanNode>().mutable_process_node()->mutable_input(0)->set_is_prepared(true);

    EXPECT_FALSE(ApplyPass<PreparedNodeBasicAnalysis>());
    EXPECT_FALSE(ApplyPass<PreparedNodeAnalysis>());

    EXPECT_FALSE(n17->has<PreparedNode>());
    EXPECT_FALSE(n16->has<PreparedNode>());
    EXPECT_EQ(n15->get<PreparedNode>().prepared_users.size(), 1);
    EXPECT_EQ(*(n15->get<PreparedNode>().prepared_users.begin()), n17);
    EXPECT_EQ(n14->get<PreparedNode>().prepared_users.size(), 1);
    EXPECT_EQ(*(n14->get<PreparedNode>().prepared_users.begin()), n15);
    EXPECT_FALSE(n13->has<PreparedNode>());
    EXPECT_EQ(n12->get<PreparedNode>().prepared_users.size(), 1);
    EXPECT_EQ(*(n12->get<PreparedNode>().prepared_users.begin()), n14);
    EXPECT_EQ(n11->get<PreparedNode>().prepared_users.size(), 1);
    EXPECT_EQ(*(n11->get<PreparedNode>().prepared_users.begin()), n12);
}

TEST_F(PreparedNodeAnalysisTest, UnionTest) {
    AddTasksOf(1);
    Unit* n11 = AddLeafBelowTask(0, "n11", Unit::LOAD_NODE);
    Unit* n12 = AddLeafBelowTask(0, "n12", Unit::PROCESS_NODE);
    Unit* n13 = AddLeafBelowTask(0, "n13", Unit::UNION_NODE);
    Unit* n14 = AddLeafBelowTask(0, "n14", Unit::PROCESS_NODE);

    SetProcess(n12);
    SetProcess(n14);

    AddInput(n11, n13, n11->identity(), false);
    AddInput(n12, n13, n12->identity(), false);
    AddInput(n13, n14, n13->identity(), false);

    n14->get<PbLogicalPlanNode>().mutable_process_node()->mutable_input(0)->set_is_prepared(true);

    EXPECT_FALSE(ApplyPass<PreparedNodeBasicAnalysis>());
    EXPECT_FALSE(ApplyPass<PreparedNodeAnalysis>());

    EXPECT_EQ(n11->get<PreparedNode>().prepared_users.size(), 1);
    EXPECT_EQ(*(n11->get<PreparedNode>().prepared_users.begin()), n13);
    EXPECT_EQ(n12->get<PreparedNode>().prepared_users.size(), 1);
    EXPECT_EQ(*(n12->get<PreparedNode>().prepared_users.begin()), n13);
    EXPECT_EQ(n13->get<PreparedNode>().prepared_users.size(), 1);
    EXPECT_EQ(*(n13->get<PreparedNode>().prepared_users.begin()), n14);
    EXPECT_FALSE(n14->has<PreparedNode>());
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu


