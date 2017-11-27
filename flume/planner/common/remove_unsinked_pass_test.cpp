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

#include "flume/planner/common/remove_unsinked_pass.h"

#include <iostream>

#include "gtest/gtest.h"

#include "flume/core/logical_plan.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/core/testing/mock_objector.h"
#include "flume/core/testing/mock_partitioner.h"
#include "flume/core/testing/mock_processor.h"
#include "flume/core/testing/mock_sinker.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/plan_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

using baidu::flume::core::LogicalPlan;

class RemoveUnsinkedPassTest : public PlanTest {};

TEST_F(RemoveUnsinkedPassTest, NoTaskNoRemoveTest) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    core::LogicalPlan::Node* n1 =
        logical_plan->Load("file1")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n2 =
        logical_plan->Load("file2")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    logical_plan->Process(n1, n2)->By<MockProcessor>()->As<MockObjector>()
        ->SinkBy<MockSinker>();

    InitFromLogicalPlan(logical_plan.get());
    Plan* plan = GetPlan();

    uint32_t old_size = plan->GetAllUnits().size();

    ApplyPass<SideEffectAnalysis>();
    bool is_changed = ApplyPass<RemoveUnsinkedPass>();
    EXPECT_FALSE(is_changed);
    EXPECT_EQ(old_size, plan->GetAllUnits().size());
}

TEST_F(RemoveUnsinkedPassTest, NoTaskRemoveAllTest) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    core::LogicalPlan::Node* n1 =
        logical_plan->Load("file1")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n2 =
        logical_plan->Load("file2")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n3 =
        logical_plan->Process(n1, n2)->By<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::ShuffleGroup* shuffle_group =
        logical_plan->Shuffle(logical_plan->global_scope(), n3);
    core::LogicalPlan::Node* n4 = shuffle_group->node(0)->MatchBy<MockKeyReader>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    InitFromLogicalPlan(logical_plan.get());
    Plan* plan = GetPlan();

    ApplyPass<SideEffectAnalysis>();
    bool is_changed = ApplyPass<RemoveUnsinkedPass>();

    EXPECT_TRUE(is_changed);
    PlanVisitor visitor(plan);
    ASSERT_FALSE(visitor.Find(n1));
    ASSERT_FALSE(visitor.Find(n2));
    ASSERT_FALSE(visitor.Find(n3));
    ASSERT_FALSE(visitor.Find(shuffle_group->node(0)));
    ASSERT_FALSE(visitor.Find(n4));
}

TEST_F(RemoveUnsinkedPassTest, RemoveAllNodeCommonScopo) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    core::LogicalPlan::Node* n1 =
        logical_plan->Load("file1")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n2 =
        logical_plan->Load("file2")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    const core::LogicalPlan::Scope* scope = n1->scope();
    core::LogicalPlan::ShuffleGroup* shuffle = logical_plan->Shuffle(scope, n1, n2);

    core::LogicalPlan::Node* n3 = shuffle->node(0)->MatchBy<MockKeyReader>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();
    core::LogicalPlan::Node* n4 = shuffle->node(1)->MatchBy<MockKeyReader>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    InitFromLogicalPlan(logical_plan.get());
    Plan* plan = GetPlan();

    ApplyPass<SideEffectAnalysis>();
    bool is_changed = ApplyPass<RemoveUnsinkedPass>();
    EXPECT_TRUE(is_changed);

    while (is_changed) {
        is_changed = ApplyPass<RemoveUnsinkedPass>();
    }

    PlanVisitor visitor(plan);
    EXPECT_FALSE(visitor.Find(n1));
    EXPECT_FALSE(visitor.Find(n2));
    EXPECT_FALSE(visitor.Find(shuffle->node(0)));
    EXPECT_FALSE(visitor.Find(shuffle->node(1)));
    EXPECT_FALSE(visitor.Find(n3));
    EXPECT_FALSE(visitor.Find(n4));
}

/* no use
TEST_F(RemoveUnsinkedPassTest, RemoveCommonScopePart) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    core::LogicalPlan::Node* n1 =
        logical_plan->Load("file1")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n2 =
        logical_plan->Load("file2")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    const core::LogicalPlan::Scope* scope = n1->scope();
    core::LogicalPlan::ShuffleGroup* shuffle = logical_plan->Shuffle(scope, n1, n2);

    core::LogicalPlan::Node* n3 = shuffle->node(0)->MatchBy<MockKeyReader>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();
    core::LogicalPlan::Node* n4 = shuffle->node(1)->MatchBy<MockKeyReader>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n5 = n3->SinkBy<MockSinker>();

    InitFromLogicalPlan(logical_plan.get());
    Plan* plan = GetPlan();

    ApplyPass<SideEffectAnalysis>();
    bool is_changed = ApplyPass<RemoveUnsinkedPass>();
    EXPECT_TRUE(is_changed);

    while (is_changed) {
        is_changed = ApplyPass<RemoveUnsinkedPass>();
    }

    PlanVisitor visitor(plan);
    //EXPECT_TRUE(visitor.Find(n1->scope()->identity()));

    //EXPECT_EQ(AsSet(n5), visitor.Find(n2).user_set());
    EXPECT_TRUE(visitor.Find(n1));
    EXPECT_TRUE(visitor.Find(n2));
    EXPECT_TRUE(visitor.Find(shuffle->node(0)));
    EXPECT_TRUE(visitor.Find(shuffle->node(1)));
    EXPECT_TRUE(visitor.Find(n3));
    EXPECT_FALSE(visitor.Find(n4));
    EXPECT_TRUE(visitor.Find(n5));
}*/

TEST_F(RemoveUnsinkedPassTest, RemovePart) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    core::LogicalPlan::Node* n1 =
        logical_plan->Load("file1")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n2 =
        logical_plan->Load("file2")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n3 =
        logical_plan->Process(n1, n2)->By<MockProcessor>()->As<MockObjector>();
    core::LogicalPlan::Node* n4 = n3->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n5 = n2->SinkBy<MockSinker>();

    InitFromLogicalPlan(logical_plan.get());
    Plan* plan = GetPlan();

    ApplyPass<SideEffectAnalysis>();
    bool is_changed = ApplyPass<RemoveUnsinkedPass>();
    EXPECT_TRUE(is_changed);

    PlanVisitor visitor(plan);
    EXPECT_TRUE(visitor.Find(n1->scope()->identity()));

    EXPECT_EQ(AsSet(n5), visitor.Find(n2).user_set());
    EXPECT_FALSE(visitor.Find(n1));
    EXPECT_TRUE(visitor.Find(n2));
    EXPECT_FALSE(visitor.Find(n3));
    EXPECT_FALSE(visitor.Find(n4));
    EXPECT_TRUE(visitor.Find(n5));
}

TEST_F(RemoveUnsinkedPassTest, RemoveSunkNode) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    core::LogicalPlan::Node* n1 =
        logical_plan->Load("file1")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n2 =
        logical_plan->Load("file2")->By<MockLoader>()->As<MockObjector>()
        ->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n3 =
        logical_plan->Process(n1, n2)->By<MockProcessor>()->As<MockObjector>();
    core::LogicalPlan::Node* n4 = n3->ProcessBy<MockProcessor>()->As<MockObjector>();

    core::LogicalPlan::Node* n5 = n2->SinkBy<MockSinker>();

    InitFromLogicalPlan(logical_plan.get());
    Plan* plan = GetPlan();
    PlanVisitor visitor(plan);
    std::string sink_id = visitor.Find(n5).unit()->identity();
    runtime::Session session;
    session.AddSunkNodeId(sink_id);
    plan->Root()->get<Session>().Assign(&session);
    ApplyPass<SideEffectAnalysis>();
    bool is_changed = ApplyPass<RemoveUnsinkedPass>();
    EXPECT_TRUE(is_changed);

    EXPECT_FALSE(visitor.Find(n1));
    EXPECT_FALSE(visitor.Find(n2));
    EXPECT_FALSE(visitor.Find(n3));
    EXPECT_FALSE(visitor.Find(n4));
    EXPECT_FALSE(visitor.Find(n5));
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
