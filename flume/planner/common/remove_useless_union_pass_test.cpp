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
// Author: Zhang Yuncong (bigflow-opensource@baidu.com)

#include "flume/planner/common/remove_useless_union_pass.h"

#include "gtest/gtest.h"

#include "flume/core/logical_plan.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/core/testing/mock_objector.h"
#include "flume/core/testing/mock_partitioner.h"
#include "flume/core/testing/mock_processor.h"
#include "flume/core/testing/mock_sinker.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/plan_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

using baidu::flume::core::LogicalPlan;

class RemoveUselessUnionPassTest : public PlanTest {};

TEST_F(RemoveUselessUnionPassTest, NoRemoveTest) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    core::LogicalPlan::Node* n1 =
        logical_plan->Load("file1")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n2 =
        logical_plan->Load("file2")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    logical_plan->Union(logical_plan->global_scope(), n1, n2)->SinkBy<MockSinker>();

    InitFromLogicalPlan(logical_plan.get());
    Plan* plan = GetPlan();

    uint32_t old_size = plan->GetAllUnits().size();
    bool is_changed = ApplyPass<RemoveUselessUnionPass>();
    EXPECT_FALSE(is_changed);
    EXPECT_EQ(old_size, plan->GetAllUnits().size());
}

TEST_F(RemoveUselessUnionPassTest, RemoveUnionTest) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    core::LogicalPlan::Node* n1 =
        logical_plan->Load("file1")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n2 =
        logical_plan->Load("file2")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n3 = logical_plan->Union(logical_plan->global_scope(), n1, n2);
    core::LogicalPlan::Node* n4 = n3->ProcessBy<MockProcessor>()->As<MockObjector>();
    core::LogicalPlan::Node* n5 = logical_plan->Union(logical_plan->global_scope(), n4);
    core::LogicalPlan::Node* n6 = n5->ProcessBy<MockProcessor>()->As<MockObjector>();
    core::LogicalPlan::Node* n7 =
        logical_plan->Process(n5, n6)->By<MockProcessor>()->As<MockObjector>();

    InitFromLogicalPlan(logical_plan.get());
    Plan* plan = GetPlan();

    bool is_changed = ApplyPass<RemoveUselessUnionPass>();

    EXPECT_TRUE(is_changed);
    PlanVisitor visit(plan);
    PlanVisitor v4 = visit.Find(n4->identity());
    EXPECT_TRUE(visit.Find(n3->identity()));
    EXPECT_FALSE(visit.Find(n5->identity()));
    EXPECT_EQ(AsSet(n6, n7), visit.Find(n4->identity()).user_set());
    const PbLogicalPlanNode& pb6 = visit.Find(n6->identity())->get<PbLogicalPlanNode>();
    const PbLogicalPlanNode& pb7 = visit.Find(n7->identity())->get<PbLogicalPlanNode>();

    EXPECT_EQ(1u, pb6.process_node().input_size());
    EXPECT_EQ(2u, pb7.process_node().input_size());

    EXPECT_EQ(n4->identity(), pb6.process_node().input(0).from());
    EXPECT_EQ(n4->identity(), pb7.process_node().input(0).from());
    EXPECT_EQ(n6->identity(), pb7.process_node().input(1).from());
}

TEST_F(RemoveUselessUnionPassTest, RemoveUnionAfterUnion) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    core::LogicalPlan::Node* n1 =
        logical_plan->Load("file1")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n2 =
        logical_plan->Load("file2")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n3 = logical_plan->Union(logical_plan->global_scope(), n1, n2);
    core::LogicalPlan::Node* n4 = n3->ProcessBy<MockProcessor>()->As<MockObjector>();
    core::LogicalPlan::Node* n5 = logical_plan->Union(logical_plan->global_scope(), n4);
    core::LogicalPlan::Node* n6 = logical_plan->Union(logical_plan->global_scope(), n5);
    core::LogicalPlan::Node* n7 = logical_plan->Union(logical_plan->global_scope(), n6);
    core::LogicalPlan::Node* n8 = n7->ProcessBy<MockProcessor>()->As<MockObjector>();
    core::LogicalPlan::Node* n9 = n7->ProcessBy<MockProcessor>()->As<MockObjector>();

    InitFromLogicalPlan(logical_plan.get());
    Plan* plan = GetPlan();

    bool is_changed = ApplyPass<RemoveUselessUnionPass>();

    EXPECT_TRUE(is_changed);
    PlanVisitor visit(plan);
    PlanVisitor v4 = visit.Find(n4->identity());
    ASSERT_TRUE(v4);
    EXPECT_TRUE(visit.Find(n3->identity()));
    EXPECT_FALSE(visit.Find(n5->identity()));
    EXPECT_FALSE(visit.Find(n6->identity()));
    EXPECT_EQ(AsSet(n8, n9), visit.Find(n4->identity()).user_set());
    EXPECT_EQ(AsSet(n4), visit.Find(n8->identity()).need_set());
    EXPECT_EQ(AsSet(n4), visit.Find(n9->identity()).need_set());
    PbLogicalPlanNode pb8 = visit.Find(n8)->get<PbLogicalPlanNode>();
    PbLogicalPlanNode pb9 = visit.Find(n9)->get<PbLogicalPlanNode>();

    EXPECT_EQ(1u, pb8.process_node().input_size());
    EXPECT_EQ(1u, pb9.process_node().input_size());

    EXPECT_EQ(n4->identity(), pb8.process_node().input(0).from());
    EXPECT_EQ(n4->identity(), pb9.process_node().input(0).from());
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
