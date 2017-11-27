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

#include "flume/planner/monitor/prepared_node_basic_analysis.h"

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

class BasicPreparedNodeRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::PROCESS_NODE;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        CHECK(unit->has<PbLogicalPlanNode>());
        const PbProcessNode& process_node = unit->get<PbLogicalPlanNode>().process_node();
        // CHECK_GT(process_node.input_size(), 0);
        std::vector<Unit*> needs = unit->direct_needs();
        for (int i = 0; i < process_node.input_size(); ++i) {
            if (!process_node.input(i).is_prepared()) {
                continue;
            }

            const std::string& from = process_node.input(i).from();
            size_t idx = 0;
            for (; idx < needs.size(); ++idx) {
                if (needs[idx]->identity() == from) {
                    SetNodePrepared(needs[idx], unit);
                    break;
                }
            }
            CHECK_LT(idx, needs.size());
        }

        return false;
    }
};

}  // namespace

bool PreparedNodeBasicAnalysis::Run(Plan* plan) {
    TopologicalDispatcher topo_dispatcher(TopologicalDispatcher::REVERSE_ORDER);
    topo_dispatcher.AddRule(new BasicPreparedNodeRule());
    topo_dispatcher.Run(plan);
    return false;
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu


