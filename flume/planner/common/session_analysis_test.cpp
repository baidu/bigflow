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
// Author: Wang Cong <bigflow-opensource@baidu.com>

#include "flume/planner/common/session_analysis.h"

#include <iostream>
#include <set>
#include <string>

#include "boost/foreach.hpp"
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
#include "flume/planner/common/tags.h"
#include "flume/planner/testing/plan_test_helper.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

using core::Entity;
using core::Loader;
using core::LogicalPlan;

class CacheUnitFilter : public filter::IFilter {
public:
    bool Filte(Unit* unit) const {
        if (!unit->has<PbLogicalPlanNode>()) {
            return false;
        }
        const PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
        return node.cache();
    }
};

FLUME_REGISTER_FILTER(CacheUnit);

// TODO(wangcong09): Refactor using new planner test framework.
class SessionAnalysisTest : public PlanTest {
public:
    typedef std::set<std::string> IdSet;
    typedef runtime::Session::NodeIdCollection IdCollection;

    void ApplySessionAnalysis(runtime::Session* session) {
        Pass* base_typed = NULL;
        SessionAnalysis analysis;

        runtime::Session tmp;
        GetPlan()->Root()->get<planner::Session>().Assign(session);
        GetPlan()->Root()->get<planner::NewSession>().Assign(&tmp);

        base_typed = &analysis;
        base_typed->Run(GetPlan());
        Plan& plan = *GetPlan();
        if (plan.Root()->has<Session>() && !plan.Root()->get<Session>().is_null()) {
            *plan.Root()->get<Session>() = *plan.Root()->get<NewSession>();
        }
    }

    void CollectSunkNodeIds(const PlanVisitor& visitor, IdSet& result) {
        PlanVisitor finds = visitor.Find(filter::type == Unit::SINK_NODE);
        const std::set<Unit*>& sink_units = finds.all_units();

        BOOST_FOREACH(Unit* unit, sink_units) {
            ASSERT_TRUE(unit->has<PbLogicalPlanNode>());
            result.insert(unit->get<PbLogicalPlanNode>().id());
        }
    }

    void CollectCachedNodeIds(const PlanVisitor& visitor, IdSet& result) {
        filter::Filter is_cached = filter::make_filter<CacheUnitFilter>();
        PlanVisitor finds = visitor.Find(is_cached);
        const std::set<Unit*>& units_with_cache = finds.all_units();

        BOOST_FOREACH(Unit* unit, units_with_cache) {
            ASSERT_TRUE(unit->has<PbLogicalPlanNode>());
            result.insert(unit->get<PbLogicalPlanNode>().id());
        }
    }

    bool EqualContains(const IdCollection& id_collection, const IdSet& id_set) {
        if (id_collection.size() != id_set.size()) {
            return false;
        }
        BOOST_FOREACH(const std::string& id, id_collection) {
            if (id_set.find(id) == id_set.end()) {
                return false;
            }
        }
        return true;
    }

    void ValidateResult(LogicalPlan* logical_plan,
                        std::set<runtime::Session>::size_type num_expected_sinks,
                        std::set<runtime::Session>::size_type num_expected_caches) {
        toft::scoped_ptr<runtime::Session> session(new runtime::Session());

        InitFromLogicalPlan(logical_plan);
        ApplySessionAnalysis(session.get());

        PlanVisitor visitor(GetPlan());
        IdSet expected_sunk_node_ids;
        IdSet expected_cached_node_ids;
        CollectSunkNodeIds(visitor, expected_sunk_node_ids);
        CollectCachedNodeIds(visitor, expected_cached_node_ids);

        const IdCollection& sunk_node_ids = session->GetSunkNodeIds();
        const IdCollection& cached_node_ids = session->GetCachedNodeIds();

        ASSERT_EQ(sunk_node_ids.size(), num_expected_sinks);
        ASSERT_EQ(cached_node_ids.size(), num_expected_caches);
        ASSERT_TRUE(EqualContains(session->GetSunkNodeIds(), expected_sunk_node_ids));
        ASSERT_TRUE(EqualContains(session->GetCachedNodeIds(), expected_cached_node_ids));
    }
};

TEST_F(SessionAnalysisTest, SessionUpdate) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());
    LogicalPlan::Node* node_1 =
            logical_plan->Load("test.txt")->By<MockLoader>()->As<MockObjector>();
    logical_plan->Sink(logical_plan->global_scope(), node_1)->By<MockSinker>();

    ValidateResult(logical_plan.get(), 1u, 0u);
}


TEST_F(SessionAnalysisTest, SessionUpdateTwice) {
    toft::scoped_ptr<LogicalPlan> logical_plan(new LogicalPlan());

    LogicalPlan::Node* node_1 = logical_plan->Load("text.txt")
                                            ->By<MockLoader>()
                                            ->As<MockObjector>();
    logical_plan->Sink(logical_plan->global_scope(), node_1)->By<MockSinker>();

    LogicalPlan::Node* node_1_1 = node_1->ProcessBy<MockProcessor>()->As<MockObjector>();
    LogicalPlan::Node* node_1_2 = node_1->ProcessBy<MockProcessor>()
                                        ->As<MockObjector>();
    node_1_2->Cache();

    LogicalPlan::ShuffleGroup* shuffle_group =
            logical_plan->Shuffle(logical_plan->global_scope(), node_1_1, node_1_2)
                ->node(0)->MatchBy<MockKeyReader>()
                ->node(1)->MatchBy<MockKeyReader>()
            ->done();

    LogicalPlan::Node* node_2_1 = shuffle_group->node(0);
    LogicalPlan::Node* node_2_2 = shuffle_group->node(1);
    LogicalPlan::Node* node_2_3 =
            node_2_1->CombineWith(node_2_2)->By<MockProcessor>()->As<MockObjector>();
    node_2_3->Cache();

    ValidateResult(logical_plan.get(), 1u, 2u);

    LogicalPlan::Node* node_3 =
            logical_plan->Process(logical_plan->global_scope(), node_1_1, node_2_3)
                ->By<MockProcessor>()->As<MockObjector>();
    node_3->Cache();

    logical_plan->Sink(logical_plan->global_scope(), node_3)->By<MockSinker>();


    ValidateResult(logical_plan.get(), 2u, 3u);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
