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
// Author: Zhang Yuncong <bigflow-opensource@baidu.com>
//

#include "flume/planner/common/remove_empty_unit_pass.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"

#include "toft/base/unordered_set.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

class RemoveEmptyUnitPassRule : public RuleDispatcher::Rule {
public:
    virtual ~RemoveEmptyUnitPassRule();

    virtual bool Accept(Plan* plan, Unit* unit);

    virtual bool Run(Plan* plan, Unit* unit);
};

RemoveEmptyUnitPassRule::~RemoveEmptyUnitPassRule() {}

bool RemoveEmptyUnitPassRule::Accept(Plan* plan, Unit* unit) {
    return !unit->is_leaf() && unit->empty() && unit != plan->Root() && !unit->has<ShouldKeep>();
}

bool RemoveEmptyUnitPassRule::Run(Plan* plan, Unit* unit) {
    plan->RemoveUnit(unit);
    return true;
}

} // namespace

RemoveEmptyUnitPass::~RemoveEmptyUnitPass() {}

bool RemoveEmptyUnitPass::Run(Plan* plan) {
    DepthFirstDispatcher dfs_dispatcher(DepthFirstDispatcher::POST_ORDER);
    dfs_dispatcher.AddRule(new RemoveEmptyUnitPassRule());
    return dfs_dispatcher.Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
