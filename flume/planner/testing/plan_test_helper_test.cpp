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
// Author: Pan Yuchang(BDG)<panyuchang@baidu.com>
// Description:

#include <algorithm>
#include <iomanip>
#include <sstream>

#include "boost/filesystem.hpp"

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

#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/testing/plan_test_helper.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

#include "toft/base/scoped_ptr.h"

namespace baidu {
namespace flume {
namespace planner {

using boost::filesystem::exists;
using boost::filesystem::path;
using baidu::flume::core::LogicalPlan;

class PlanTesterTest : public PlanTest {};

TEST_F(PlanTesterTest, BaseTest1) {
    Unit* m_u[8];
    AddTasksOf(3);
    m_u[0] = AddLeafBelowTask(0, "n0", Unit::PROCESS_NODE);
    m_u[1] = AddLeafBelowTask(1, "n1", Unit::PROCESS_NODE);
    m_u[2] = AddNodeBelowTask(2, "n2", Unit::SHUFFLE_EXECUTOR);
    m_u[3] = AddLeaf(m_u[2], "n3", Unit::SHUFFLE_NODE);
    m_u[4] = AddLeaf(m_u[2], "n4", Unit::PROCESS_NODE);
    m_u[5] = AddNode(m_u[2], "n5", Unit::SHUFFLE_EXECUTOR);
    m_u[6] = AddLeaf(m_u[5], "n6", Unit::SHUFFLE_NODE);
    m_u[7] = AddLeaf(m_u[5], "n7", Unit::PROCESS_NODE);
    AddDependency(m_u[0], m_u[1]);
    AddDependency(m_u[1], m_u[3]);
    AddDependency(m_u[3], m_u[4]);
    AddDependency(m_u[3], m_u[6]);
    AddDependency(m_u[6], m_u[7]);

    const int MaxCount = 4;
    for (int i = 0; i < MaxCount - 1; ++i) {
        EXPECT_FALSE(ApplyPass<DataFlowAnalysis>());
    }

    path dir_path("./dot/BaseTest1");
    for (int i = 0; i < MaxCount; ++i) {
        std::ostringstream stream;
        stream << std::setfill('0') << std::setw(3) << i << ".dot";
        path file_path = dir_path / stream.str();
        EXPECT_TRUE(exists(file_path));
    }
}

TEST_F(PlanTesterTest, BaseTest2) {
    Unit* m_u[8];
    AddTasksOf(3);
    m_u[0] = AddLeafBelowTask(0, "n0", Unit::PROCESS_NODE);
    m_u[1] = AddLeafBelowTask(1, "n1", Unit::PROCESS_NODE);
    m_u[2] = AddNodeBelowTask(2, "n2", Unit::SHUFFLE_EXECUTOR);
    m_u[3] = AddLeaf(m_u[2], "n3", Unit::SHUFFLE_NODE);
    m_u[4] = AddLeaf(m_u[2], "n4", Unit::PROCESS_NODE);
    m_u[5] = AddNode(m_u[2], "n5", Unit::SHUFFLE_EXECUTOR);
    m_u[6] = AddLeaf(m_u[5], "n6", Unit::SHUFFLE_NODE);
    m_u[7] = AddLeaf(m_u[5], "n7", Unit::PROCESS_NODE);
    AddDependency(m_u[0], m_u[1]);
    AddDependency(m_u[1], m_u[3]);
    AddDependency(m_u[3], m_u[4]);
    AddDependency(m_u[3], m_u[6]);
    AddDependency(m_u[6], m_u[7]);


    const int MaxCount = 20;
    for (int i = 0; i < MaxCount - 1; ++i) {
        EXPECT_FALSE(ApplyPass<DataFlowAnalysis>());
    }

    path dir_path("./dot/BaseTest2");
    for (int i = 0; i < MaxCount; ++i) {
        std::ostringstream stream;
        stream << std::setfill('0') << std::setw(3) << i << ".dot";
        path file_path = dir_path / stream.str();
        EXPECT_TRUE(exists(file_path));
    }
}

TEST_F(PlanTesterTest, BaseTest3) {
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

    logical_plan->Process(logical_plan->global_scope(), node_1_1, node_2_3)
            ->By<MockProcessor>()->As<MockObjector>();

    InitFromLogicalPlan(logical_plan.get());

    PlanVisitor root = GetPlanVisitor();
    ASSERT_EQ(3, root->size());

    const int MaxCount = 5;
    for (int i = 0; i < MaxCount - 1; ++i) {
        EXPECT_FALSE(ApplyPass<DataFlowAnalysis>());
    }

    path dir_path("./dot/BaseTest3");
    for (int i = 0; i < MaxCount; ++i) {
        std::ostringstream stream;
        stream << std::setfill('0') << std::setw(3) << i << ".dot";
        path file_path = dir_path / stream.str();
        EXPECT_TRUE(exists(file_path));
    }
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
