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
// Author: Zhou Kai <bigflow-opensource@baidu.com>

#include "flume/planner/topological_dispatcher.h"

#include <algorithm>
#include <vector>

#include "flume/planner/plan.h"

namespace baidu {
namespace flume {
namespace planner {

TopologicalDispatcher::TopologicalDispatcher(bool reversed)
        : m_reversed(reversed),
        m_only_apply_first_target(false),
        m_include_control_units(false) {}

void TopologicalDispatcher::IncludeControlUnits(bool include_control_units) {
    m_include_control_units = include_control_units;
}

void TopologicalDispatcher::OnlyApplyFirstTarget(bool only_apply_first) {
    m_only_apply_first_target = only_apply_first;
}

bool TopologicalDispatcher::Run(Plan* plan) {
    bool changed = false;
    std::vector<Unit*> topo_order = plan->GetTopologicalOrder(m_only_apply_first_target);
    if (m_reversed) {
        std::reverse(topo_order.begin(), topo_order.end());
    }
    for (size_t i = 0; i < topo_order.size(); ++i) {
        Unit* unit = topo_order[i];
        changed |= Dispatch(plan, unit);
        if (changed && m_only_apply_first_target) {
            return true;
        }
    }
    return changed;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
