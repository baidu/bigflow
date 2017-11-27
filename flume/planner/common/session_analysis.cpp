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

#include <algorithm>
#include <deque>
#include <vector>

#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

class AddCacheNodeIdRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        if (!unit->has<PbLogicalPlanNode>()) {
            return false;
        }
        const PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
        return node.cache();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        CHECK(unit->father() != NULL);
        CHECK(unit->father()->has<PbScope>());

        runtime::Session* session = plan->Root()->get<Session>();
        runtime::Session* new_session = plan->Root()->get<NewSession>();

        if (session->IsCachedNodeIn(unit->identity())) {
            // already cached
            unit->set_type(Unit::CACHE_READER);
        }
        new_session->AddCachedNodeId(unit->identity());
        return false;
    }
};

class AddSinkNodeIdRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return Unit::SINK_NODE == unit->type();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        const PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
        runtime::Session* session = plan->Root()->get<NewSession>();
        session->AddSunkNodeId(node.id());
        return false;
    }
};

class SetNodesMapRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->has<PbLogicalPlanNode>();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        const PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
        runtime::Session* session = plan->Root()->get<NewSession>();
        session->GetNodesMap()[node.id()] = node;
        return false;
    }
};

}  // namespace

bool SessionAnalysis::Run(Plan* plan) {
    if (!plan->Root()->has<Session>()
            || plan->Root()->get<Session>().is_null()) {
        LOG(INFO) << "this should not have happened except in the unittest!!!";
        static runtime::Session session;
        plan->Root()->get<Session>().Assign(&session);
    }

    if (!plan->Root()->has<NewSession>()
            || plan->Root()->get<NewSession>().is_null()) {
        LOG(INFO) << "this should not have happened except in the unittest!!!";
        static runtime::Session new_session;
        plan->Root()->get<NewSession>().Assign(&new_session);
    }

    DepthFirstDispatcher dfs_dispatcher(DepthFirstDispatcher::POST_ORDER);
    dfs_dispatcher.AddRule(new AddCacheNodeIdRule());
    dfs_dispatcher.AddRule(new AddSinkNodeIdRule());
    dfs_dispatcher.AddRule(new SetNodesMapRule());
    return dfs_dispatcher.Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
