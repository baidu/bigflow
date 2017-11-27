/***************************************************************************
 *
 * Copyright (c) 2014 Baidu, Inc. All Rights Reserved.
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
// Author: Wang Cong <bigflow-opensource@baidu.com>

#include "flume/planner/common/check_cycle_pass.h"

#include <algorithm>

#include "glog/logging.h"

#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

typedef DataFlowAnalysis::Info Info;

class CheckCycleRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit);
    virtual bool Run(Plan* plan, Unit* unit);
};

bool CheckCycleRule::Accept(Plan* plan, Unit* unit) {
    return (!unit->is_leaf() && unit->has<PbScope>());
}

bool CheckCycleRule::Run(Plan* plan, Unit* unit) {
    CHECK(unit->has<PbScope>()) << "Trying to check cycle for a non-scope unit";

    Info::NodeSet& downstreams = unit->get<Info>().downstreams;
    Info::NodeSet udod;  // Union of downstreams of current unit's downstreams
    for (Info::NodeSet::iterator it = downstreams.begin(); it != downstreams.end(); ++it) {
        Info& info = (*it)->get<Info>();
        udod.insert(info.downstreams.begin(), info.downstreams.end());
    }

    Info::NodeSet& nodes = unit->get<Info>().nodes;
    std::vector<Unit*> intersection;
    std::set_intersection(udod.begin(), udod.end(),
                          nodes.begin(), nodes.end(),
                          std::inserter(intersection, intersection.end()));
    CHECK(0u == intersection.size()) << "Scope cycle is found in current plan: "
                                     << std::endl
                                     << "Scope: "
                                     << unit->get<PbScope>().DebugString()
                                     << std::endl;
    return false;
}

}  // namespace

bool CheckCyclePass::Run(Plan* plan) {
    DepthFirstDispatcher cycle_checker(DepthFirstDispatcher::POST_ORDER);
    cycle_checker.AddRule(new CheckCycleRule());
    cycle_checker.Run(plan);

    return false;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
