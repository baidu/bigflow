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
// Author: Guo Yezhi <bigflow-opensource@baidu.com>

#include "flume/planner/common/draw_plan_pass.h"

#include "gtest/gtest.h"

#include "flume/core/logical_plan.h"
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

using core::LogicalPlan;

void PrintDot(const std::string& dot) {
    LOG(INFO) << "DOT:\n" << dot;
}

class DrawPlanPassTest : public PlanTest {};

TEST_F(DrawPlanPassTest, BasicTest) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    LogicalPlan::Node* node_1 =
            logical_plan->Load("test.txt")->By<MockLoader>()->As<MockObjector>();
    LogicalPlan::Node* node_2 =
            logical_plan->Sink(logical_plan->global_scope(), node_1)->By<MockSinker>();
    ASSERT_TRUE(node_2 != NULL);

    InitFromLogicalPlan(logical_plan.get());
    Plan* plan = GetPlan();

    DrawPlanPass drawer;
    drawer.RegisterListener(toft::NewPermanentClosure(&PrintDot));
    EXPECT_FALSE(drawer.Run(plan));
}

TEST_F(DrawPlanPassTest, DisplayMessage) {
    AddTasksOf(2);
    PlanVisitor root = GetPlanVisitor();

    Plan* plan = GetPlan();
    DrawPlanPass drawer;
    drawer.RegisterListener(toft::NewPermanentClosure(&PrintDot));

    DrawPlanPass::AddMessage(root.unit(), "Hello\n<World>!");

    EXPECT_FALSE(drawer.Run(plan));

    EXPECT_FALSE(drawer.Run(plan));
}

} // namespace planner
} // namespace flume
} // namespace baidu

