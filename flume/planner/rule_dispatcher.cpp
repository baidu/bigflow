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

#include "flume/planner/rule_dispatcher.h"

#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

bool RuleDispatcher::Run(Plan* plan) {
    bool changed = false;
    std::vector<Unit*> units = plan->GetAllUnits();
    for (size_t i = 0; i < units.size(); ++i) {
        Unit* unit = units[i];
        changed |= Dispatch(plan, unit);
    }
    return changed;
}

void RuleDispatcher::AddRule(Rule* rule) {
    m_rules.push_back(rule);
}

bool RuleDispatcher::Dispatch(Plan* plan, Unit* unit) {
    bool changed = false;
    for (size_t i = 0; i < m_rules.size(); ++i) {
        Rule& rule = m_rules[i];
        if (!unit->is_discard() && rule.Accept(plan, unit)) {
            changed |= rule.Run(plan, unit);
        }
    }
    return changed;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
