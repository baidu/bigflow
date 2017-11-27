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
// Author: Wang Song <bigflow-opensource@baidu.com>
//

#include "flume/planner/monitor/prepared_node_analysis.h"

#include "boost/foreach.hpp"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

namespace {

void SetNodePrepared(Unit* node, Unit* user) {
    node->get<PreparedNode>().prepared_users.insert(user);
    DrawPlanPass::UpdateLabel(node, "01-is-prepared", "Is Prepared");
}

class LatentPreparedNodeRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->is_leaf();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        if (unit->type() == Unit::UNION_NODE && unit->has<PreparedNode>()) {
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                SetNodePrepared(need, unit);
            }
            return false;
        }

        std::vector<Unit*> users = unit->direct_users();
        if (unit->type() == Unit::SHUFFLE_NODE) {
            BOOST_FOREACH(Unit* user, users) {
                if (user->has<PreparedNode>()) {
                    SetNodePrepared(unit, user);
                    break;
                }
            }
            return false;
        }

        if ((users.size() == 1) &&
                users[0]->has<PreparedNode>() &&
                (users[0]->direct_needs().size() == 1)) {
            SetNodePrepared(unit, users[0]);
        }

        return false;
    }
};

}  // namespace

bool PreparedNodeAnalysis::Run(Plan* plan) {
    TopologicalDispatcher topo_dispatcher2(TopologicalDispatcher::REVERSE_ORDER);
    topo_dispatcher2.AddRule(new LatentPreparedNodeRule());
    topo_dispatcher2.Run(plan);

    return false;
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu


