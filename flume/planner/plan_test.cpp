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
// Author: Zhou Kai <zhoukai01@baidu.com>

#include "flume/planner/plan.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "boost/shared_ptr.hpp"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/scoped_ptr.h"

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
#include "flume/planner/graph_helper.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

using ::testing::AllOf;
using ::testing::Contains;
using ::testing::ElementsAre;

using core::Entity;
using core::Loader;
using core::LogicalPlan;

/*
struct ScopeLevel {
    ScopeLevel() : level(0) {}

    int level;
};

struct ScopeLevelValue {
    ScopeLevelValue() : level(0), value(0) {}

    int level;
    int value;
};

void SetScopeLevel(Plan::Unit* unit) {
    if (unit->father == NULL) {
        unit->Get<ScopeLevel>().level = 0;
    } else if (unit->type == Plan::Unit::SCOPE) {
        unit->Get<ScopeLevel>().level = unit->father->Get<ScopeLevel>().level + 1;
    } else {
        unit->Get<ScopeLevel>().level = unit->father->Get<ScopeLevel>().level;
    }

    std::set<Plan::Unit*>::iterator iter;
    for (iter = unit->childs.begin(); iter != unit->childs.end(); ++iter) {
        SetScopeLevel(*iter);
    }
}

TEST(PlanTest, ScopeLevel) {
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

    LogicalPlan::Node* node_3 = logical_plan
            ->Process(logical_plan->global_scope(), node_1_1, node_2_3)
            ->By<MockProcessor>()->As<MockObjector>();
    CHECK_NOTNULL(node_3);

    PbLogicalPlan pb = logical_plan->ToProtoMessage();
    Plan plan(pb);

    Plan::Unit* root = plan.Root();
    root->Get<ScopeLevel>().level = 0;
    SetScopeLevel(root);
    Plan::Unit* unit_2_3 = plan.At(node_2_3->identity());
    EXPECT_EQ(1, unit_2_3->Get<ScopeLevel>().level);
    root->GetValue<ScopeLevelValue>() = 5;
    EXPECT_EQ(5, root->GetValue<ScopeLevelValue>());
}

struct InsideNodes {
    std::set<Plan::Unit*> total;
};

void CompleteScopeInfo(Plan::Unit* unit) {
    std::set<Plan::Unit*>::iterator iter;
    for (iter = unit->childs.begin(); iter != unit->childs.end(); ++iter) {
        Plan::Unit* child = *iter;
        if (child->type == Plan::Unit::NODE) {
            unit->Get<InsideNodes>().total.insert(child);
        } else {
            CompleteScopeInfo(child);
            std::set<Plan::Unit*>& total = child->Get<InsideNodes>().total;
            unit->Get<InsideNodes>().total.insert(total.begin(), total.end());
        }
    }
}

TEST(PlanTest, NodeCover) {
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

    PbLogicalPlan pb = logical_plan->ToProtoMessage();
    Plan plan(pb);

    Plan::Unit* root = plan.Root();
    CompleteScopeInfo(root);
    EXPECT_EQ(7u, root->Get<InsideNodes>().total.size());
}
*/

}  // namespace planner
}  // namespace flume
}  // namespace baidu
