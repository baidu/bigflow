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
// Author: Wen Xiang <wenxiang@baidu.com>

#include "flume/planner/common/load_logical_plan_pass.h"

#include "gtest/gtest.h"

#include "flume/core/entity.h"
#include "flume/core/key_reader.h"
#include "flume/core/loader.h"
#include "flume/core/logical_plan.h"
#include "flume/core/objector.h"
#include "flume/core/processor.h"
#include "flume/core/sinker.h"
#include "flume/core/testing/mock_key_reader.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/core/testing/mock_objector.h"
#include "flume/core/testing/mock_processor.h"
#include "flume/core/testing/mock_sinker.h"
#include "flume/planner/plan.h"
#include "flume/planner/testing/plan_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

using core::Entity;
using core::Loader;
using core::LogicalPlan;

using ::testing::ElementsAre;

void ExpectIs(const PlanVisitor& visitor, const LogicalPlan::Node* node) {
    EXPECT_EQ(0u, visitor->size());
    EXPECT_TRUE(visitor->is_leaf());
    EXPECT_EQ(node->identity(), visitor->get<PbLogicalPlanNode>().id());
    EXPECT_FALSE(visitor->has<PbScope>());
}

void ExpectIs(const PlanVisitor& visitor, const LogicalPlan::Scope* scope) {
    EXPECT_FALSE(visitor->is_leaf());
    EXPECT_FALSE(visitor->has<PbLogicalPlanNode>());
    EXPECT_EQ(scope->identity(), visitor->get<PbScope>().id());
    EXPECT_EQ(kEmptySet, visitor.need_set());
    EXPECT_EQ(kEmptySet, visitor.user_set());
}

class LoadLogicalPlanPassTest : public PlanTest {};

TEST_F(LoadLogicalPlanPassTest, LoadSink) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    LogicalPlan::Node* node_1 =
            logical_plan->Load("test.txt")->By<MockLoader>()->As<MockObjector>();
    LogicalPlan::Node* node_2 =
            logical_plan->Sink(logical_plan->global_scope(), node_1)->By<MockSinker>();

    InitFromLogicalPlan(logical_plan.get());
    PlanVisitor root = GetPlanVisitor();
    ASSERT_EQ(2, root->size());
    EXPECT_EQ(Unit::SCOPE, root->type());
    ExpectIs(root, logical_plan->global_scope());

    PlanVisitor input = root[node_1->scope()];
    ASSERT_EQ(1, input->size());
    EXPECT_EQ(Unit::SCOPE, input->type());
    ExpectIs(input, node_1->scope());

    PlanVisitor load = input[node_1];
    ASSERT_EQ(0, load->size());
    EXPECT_EQ(Unit::LOAD_NODE, load->type());
    ExpectIs(load, node_1);
    EXPECT_EQ(kEmptySet, load.need_set());
    EXPECT_EQ(AsSet(node_2), load.user_set());

    PlanVisitor sink = root[node_2];
    ASSERT_EQ(0, sink->size());
    EXPECT_EQ(Unit::SINK_NODE, sink->type());
    ExpectIs(sink, node_2);
    EXPECT_EQ(AsSet(node_1), sink.need_set());
    EXPECT_EQ(kEmptySet, sink.user_set());
}

TEST_F(LoadLogicalPlanPassTest, ShuffleUnion) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());

    LogicalPlan::Node* node_1 =
            logical_plan->Load("test.txt")->By<MockLoader>()->As<MockObjector>();
    LogicalPlan::Node* node_2 = node_1->GroupBy<MockKeyReader>("1");
    LogicalPlan::Node* node_3 =
            logical_plan->Union(logical_plan->global_scope(), node_1, node_2);

    InitFromLogicalPlan(logical_plan.get());
    PlanVisitor root = GetPlanVisitor();
    ASSERT_EQ(2, root->size());

    PlanVisitor input = root[node_1->scope()];
    ASSERT_EQ(2, input->size());

    PlanVisitor shuffle_executor = input[node_2->scope()];
    ASSERT_EQ(1u, shuffle_executor->size());
    EXPECT_EQ(Unit::SCOPE, shuffle_executor->type());
    ExpectIs(shuffle_executor, node_2->scope());

    PlanVisitor shuffle = shuffle_executor[node_2];
    EXPECT_EQ(0, shuffle->size());
    EXPECT_EQ(Unit::SHUFFLE_NODE, shuffle->type());
    ExpectIs(shuffle, node_2);
    EXPECT_EQ(AsSet(node_1), shuffle.need_set());
    EXPECT_EQ(AsSet(node_3), shuffle.user_set());

    PlanVisitor union_ = root[node_3];
    EXPECT_EQ(0, union_->size());
    EXPECT_EQ(Unit::UNION_NODE, union_->type());
    ExpectIs(union_, node_3);
    EXPECT_EQ(AsSet(node_1, node_2), union_.need_set());
    EXPECT_EQ(kEmptySet, union_.user_set());
}

TEST_F(LoadLogicalPlanPassTest, Process) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());

    LogicalPlan::Node* node_1 =
            logical_plan->Load("test.txt")->By<MockLoader>()->As<MockObjector>();
    LogicalPlan::Node* node_1_1 = node_1->ProcessBy<MockProcessor>()->As<MockObjector>();
    LogicalPlan::Node* node_1_2 = node_1->ProcessBy<MockProcessor>()->As<MockObjector>();

    LogicalPlan::ShuffleGroup* shuffle_group =
            logical_plan->Shuffle(logical_plan->global_scope(), node_1_1, node_1_2)
                    ->node(0)->MatchBy<MockKeyReader>()
                    ->node(1)->MatchBy<MockKeyReader>()
            ->done();
    LogicalPlan::Node* node_2_1 = shuffle_group->node(0);
    LogicalPlan::Node* node_2_2 = shuffle_group->node(1);
    LogicalPlan::Node* node_2_3 =
            node_2_1->CombineWith(node_2_2)->By<MockProcessor>()->As<MockObjector>();

    LogicalPlan::Node* node_3 =
        logical_plan->Process(logical_plan->global_scope(), node_1_1, node_2_3)
            ->By<MockProcessor>()->As<MockObjector>();

    InitFromLogicalPlan(logical_plan.get());
    PlanVisitor root = GetPlanVisitor();
    ExpectIs(root, logical_plan->global_scope());
    ASSERT_EQ(3, root->size());

    PlanVisitor input = root[node_1->scope()];
    ExpectIs(input, node_1->scope());
    ASSERT_EQ(3, input->size());

    PlanVisitor shuffle_executor = root[shuffle_group->scope()];
    ExpectIs(shuffle_executor, shuffle_group->scope());
    ASSERT_EQ(3, shuffle_executor->size());

    PlanVisitor process_1_1 = input[node_1_1];
    ExpectIs(process_1_1, node_1_1);
    EXPECT_EQ(Unit::PROCESS_NODE, process_1_1->type());
    EXPECT_EQ(AsSet(node_1), process_1_1.need_set());
    EXPECT_EQ(AsSet(node_2_1, node_3), process_1_1.user_set());

    PlanVisitor process_1_2 = input[node_1_2];
    ExpectIs(process_1_2, node_1_2);
    EXPECT_EQ(Unit::PROCESS_NODE, process_1_2->type());
    EXPECT_EQ(AsSet(node_1), process_1_2.need_set());
    EXPECT_EQ(AsSet(node_2_2), process_1_2.user_set());

    PlanVisitor process_2 = shuffle_executor[node_2_3];
    ExpectIs(process_2, node_2_3);
    EXPECT_EQ(Unit::PROCESS_NODE, process_2->type());
    EXPECT_EQ(AsSet(node_2_1, node_2_2), process_2.need_set());
    EXPECT_EQ(AsSet(node_3), process_2.user_set());

    PlanVisitor process_3 = root[node_3];
    ExpectIs(process_3, node_3);
    EXPECT_EQ(Unit::PROCESS_NODE, process_3->type());
    EXPECT_EQ(AsSet(node_1_1, node_2_3), process_3.need_set());
    EXPECT_EQ(kEmptySet, process_3.user_set());
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
