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
// Author: Zheng Gonglin <bigflow-opensource@baidu.com>

#include "flume/planner/common/broadcast_shuffle_node_under_scope_analysis.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"

#include "boost/foreach.hpp"
#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace planner {

class BroadcastShuffleNodeUnderScopeAnalysisRule: public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit);
    virtual bool Run(Plan* plan, Unit* unit);
private:
    bool IsBroadcastShuffleNode(Unit* unit);
};

bool BroadcastShuffleNodeUnderScopeAnalysisRule::IsBroadcastShuffleNode(Unit* unit) {
    const PbLogicalPlanNode& message = unit->get<PbLogicalPlanNode>();
    if (message.type() == PbLogicalPlanNode::SHUFFLE_NODE) {
        const PbShuffleNode& node = message.shuffle_node();
        if (node.type() == PbShuffleNode::BROADCAST) {
            return true;
        }
    }
    return false;
}

bool BroadcastShuffleNodeUnderScopeAnalysisRule::Accept(Plan* plan, Unit* unit) {
    return true;
}

bool BroadcastShuffleNodeUnderScopeAnalysisRule::Run(Plan* plan, Unit* unit) {
    if (IsBroadcastShuffleNode(unit)) {
        Unit* father = unit->father();
        if (father->type() == Unit::SCOPE) {
            father->set<HasBroadcastShuffleNode>();
        }
    }
    return false;
}

bool BroadcastShuffleNodeUnderScopeAnalysis::Run(Plan* plan) {
    DepthFirstDispatcher control_dispatcher(DepthFirstDispatcher::POST_ORDER);
    control_dispatcher.AddRule(new BroadcastShuffleNodeUnderScopeAnalysisRule());
    control_dispatcher.Run(plan);

    return false;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
