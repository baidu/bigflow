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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>
//

#include "flume/planner/common/remove_useless_union_pass.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "flume/planner/common/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"

#include "toft/base/unordered_set.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

bool IsMaterializedUnit(Unit* unit) {
    if (!unit->has<PbLogicalPlanNode>()) {
        return false;
    }
    const PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
    return node.cache();
}

class RemoveUselessUnionPassRule : public RuleDispatcher::Rule {
public:
    virtual ~RemoveUselessUnionPassRule();

    virtual bool Accept(Plan* plan, Unit* unit);

    virtual bool Run(Plan* plan, Unit* unit);
};

RemoveUselessUnionPassRule::~RemoveUselessUnionPassRule() {}

bool RemoveUselessUnionPassRule::Accept(Plan* plan, Unit* unit) {
    return !IsMaterializedUnit(unit)
            && Unit::UNION_NODE == unit->type()
            && unit->get<PbLogicalPlanNode>().union_node().from_size() == 1
            && unit->direct_needs().size() == 1
            && !unit->has<MustKeep>();
}

bool RemoveUselessUnionPassRule::Run(Plan* plan, Unit* unit) {
    Unit* need = unit->direct_needs()[0];
    std::vector<Unit*> users = unit->direct_users();
    for (size_t i = 0; i != users.size(); ++i) {
        plan->AddDependency(need, users[i]);
        plan->ReplaceFrom(users[i], unit->identity(), need->identity());
    }
    plan->RemoveUnit(unit);
    return true;
}

} // namespace

RemoveUselessUnionPass::~RemoveUselessUnionPass() {}

bool RemoveUselessUnionPass::Run(Plan* plan) {
    TopologicalDispatcher topo_dispatcher(false);
    topo_dispatcher.AddRule(new RemoveUselessUnionPassRule());
    return topo_dispatcher.Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
