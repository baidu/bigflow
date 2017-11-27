/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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
// Author: Zhang Yuncong <bigflow-opensource@baidu.com>

#include "flume/planner/common/side_effect_analysis.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "flume/planner/common/clean_tag_rule.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

class  SideEffectAnalysisRule: public RuleDispatcher::Rule {
public:

    virtual bool Accept(Plan* plan, Unit* unit) {

        if (unit->type() == Unit::SINK_NODE) {
            Session session = plan->Root()->get<Session>();
            std::set<std::string> sunk_nodes = session->GetSunkNodeIds();
            bool unsunk = sunk_nodes.find(unit->identity()) == sunk_nodes.end();
            return unsunk;
        }

        if (unit->has<ShouldCache>()) {
            return true;
        }

        if (unit->has<ShouldKeep>()) {
            return true;
        }

        return unit->type() == Unit::CACHE_WRITER // CacheWriterExecutor
            || (unit->type() == Unit::CHANNEL && unit->father()->type() == Unit::CACHE_WRITER);
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        if (unit->has<HasSideEffect>()) {
            return false;
        }

        unit->get<HasSideEffect>(); // set flag
        return true;
    }
};

}  // namespace

bool SideEffectAnalysis::Run(Plan* plan) {
    if (!plan->Root()->has<Session>()
            || plan->Root()->get<Session>().is_null()) {
        LOG(INFO) << "Session not set, ignore pass...";
        return false;
    }

    DepthFirstDispatcher dfs_dispatcher(DepthFirstDispatcher::POST_ORDER);
    dfs_dispatcher.AddRule(new CleanTagRule<HasSideEffect>());
    dfs_dispatcher.AddRule(new SideEffectAnalysisRule());
    return dfs_dispatcher.Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
