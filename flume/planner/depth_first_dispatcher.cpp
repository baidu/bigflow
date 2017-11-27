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
//         Wen Xiang <wenxiang@baidu.com>

#include "flume/planner/depth_first_dispatcher.h"

#include <vector>

#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

DepthFirstDispatcher::DepthFirstDispatcher(Type type, bool only_apply_one_target)
        : m_type(type),
        m_only_apply_one_target(only_apply_one_target){}

bool DepthFirstDispatcher::Run(Plan* plan) {
    std::vector<Unit*> order;
    if (m_type == PRE_ORDER) {
        AddUnitByPreorder(plan->Root(), &order);
    } else {
        AddUnitByPostorder(plan->Root(), &order);
    }

    bool changed = false;
    for (size_t i = 0; i < order.size(); ++i) {
        changed |= Dispatch(plan, order[i]);
        if (changed && m_only_apply_one_target) {
            return true;
        }
    }
    return changed;
}

void DepthFirstDispatcher::AddUnitByPreorder(Unit* unit, std::vector<Unit*>* order) {
    order->push_back(unit);
    for (Unit::iterator it = unit->begin(); it != unit->end(); ++it) {
        AddUnitByPreorder(*it, order);
    }
}

void DepthFirstDispatcher::AddUnitByPostorder(Unit* unit, std::vector<Unit*>* order) {
    for (Unit::iterator it = unit->begin(); it != unit->end(); ++it) {
        AddUnitByPostorder(*it, order);
    }
    order->push_back(unit);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
