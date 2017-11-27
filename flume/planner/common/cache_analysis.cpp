/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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

// Author: daiweiwei01@baidu.com

#include "flume/planner/common/cache_analysis.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "flume/core/logical_plan.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/common/draw_plan_pass.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

class MarkCacheRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        if (!unit->has<PbLogicalPlanNode>()) {
            return false;
        }
        const PbLogicalPlanNode &node = unit->get<PbLogicalPlanNode>();
        return node.cache() && unit->type() != Unit::CACHE_READER;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        unit->set<ShouldCache>();
        DrawPlanPass::UpdateLabel(unit, "07-should-cache", "Should Cache");
        return false;
    }
};

}  // namespace

bool CacheAnalysis::Run(Plan* plan) {
    DepthFirstDispatcher dfs_dispatcher(DepthFirstDispatcher::POST_ORDER);
    dfs_dispatcher.AddRule(new MarkCacheRule());
    return dfs_dispatcher.Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
