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

#include "flume/planner/common/data_flow_analysis.h"

#include <algorithm>

#include "glog/logging.h"

#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

typedef DataFlowAnalysis::Info Info;

class DataFlowAnalysisRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit);
    virtual bool Run(Plan* plan, Unit* unit);

private:
    void ClearNodeSet(Unit* unit);
    void InitNodeSet(Plan* plan, Unit* unit);
    void UpdateOutputs(Plan* plan, Unit* unit);
    void UpdateInputs(Plan* plan, Unit* unit);
    Info::NodeSet Diff(const Info::NodeSet& set1, const Info::NodeSet& set2);
};

class UpstreamRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit);
    virtual bool Run(Plan* plan, Unit* unit);
};

class DownstreamRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit);
    virtual bool Run(Plan* plan, Unit* unit);
};

bool DataFlowAnalysisRule::Accept(Plan* plan, Unit* unit) {
    return true;
}

bool DataFlowAnalysisRule::Run(Plan* plan, Unit* unit) {
    ClearNodeSet(unit);
    InitNodeSet(plan, unit);

    Info& info = unit->get<Info>();
    for (Unit::iterator it = unit->begin(); it != unit->end(); ++it) {
        Info& child = (*it)->get<Info>();
        info.nodes.insert(child.nodes.begin(), child.nodes.end());
        info.users.insert(child.users.begin(), child.users.end());
        info.needs.insert(child.needs.begin(), child.needs.end());
        info.upstreams.insert(child.upstreams.begin(), child.upstreams.end());
        info.downstreams.insert(child.downstreams.begin(), child.downstreams.end());
    }

    info.users = Diff(info.users, info.nodes);
    info.needs = Diff(info.needs, info.nodes);
    info.upstreams = Diff(info.upstreams, info.nodes);
    info.downstreams = Diff(info.downstreams, info.nodes);

    UpdateOutputs(plan, unit);
    UpdateInputs(plan, unit);

    return false;
}

void DataFlowAnalysisRule::ClearNodeSet(Unit* unit) {
    Info& info = unit->get<Info>();
    info.nodes.clear();
    info.users.clear();
    info.needs.clear();
    info.inputs.clear();
    info.outputs.clear();

    if (!unit->is_leaf()) {
        info.upstreams.clear();
        info.downstreams.clear();
    }
}

void DataFlowAnalysisRule::InitNodeSet(Plan* plan, Unit* unit) {
    Info& info = unit->get<Info>();

    std::vector<Unit*> users = unit->direct_users();
    info.users.insert(users.begin(), users.end());

    std::vector<Unit*> needs = unit->direct_needs();
    info.needs.insert(needs.begin(), needs.end());

    if (!users.empty() || !needs.empty()) {
        info.nodes.insert(unit);
    }
}

void DataFlowAnalysisRule::UpdateOutputs(Plan* plan, Unit* unit) {
    Info& info = unit->get<Info>();
    for (Info::NodeSet::iterator it = info.users.begin(); it != info.users.end(); ++it) {
        Unit* target = *it;
        std::vector<Unit*> sources = target->direct_needs();
        for (size_t i = 0; i < sources.size(); ++i) {
            Unit* source = sources[i];
            if (info.nodes.count(source) > 0) {
                info.outputs.push_back(std::make_pair(source, target));
            }
        }
    }
}

void DataFlowAnalysisRule::UpdateInputs(Plan* plan, Unit* unit) {
    Info& info = unit->get<Info>();
    for (Info::NodeSet::iterator it = info.needs.begin(); it != info.needs.end(); ++it) {
        Unit* source = *it;
        std::vector<Unit*> targets = source->direct_users();
        for (size_t i = 0; i < targets.size(); ++i) {
            Unit* target = targets[i];
            if (info.nodes.count(target) > 0) {
                info.inputs.push_back(std::make_pair(source, target));
            }
        }
    }
}

Info::NodeSet DataFlowAnalysisRule::Diff(const Info::NodeSet& set1, const Info::NodeSet& set2) {
    Info::NodeSet result;
    std::set_difference(set1.begin(), set1.end(),
                        set2.begin(), set2.end(),
                        std::inserter(result, result.end()));
    return result;
}

bool UpstreamRule::Accept(Plan* plan, Unit* unit) {
    return unit->is_leaf();
}

bool UpstreamRule::Run(Plan* plan, Unit* unit) {
    Info& info = unit->get<Info>();
    info.upstreams.clear();

    std::vector<Unit*> needs = unit->direct_needs();
    for (size_t i = 0; i < needs.size(); ++i) {
        Unit* up_node = needs[i];
        Info::NodeSet& upstreams = up_node->get<Info>().upstreams;
        info.upstreams.insert(upstreams.begin(), upstreams.end());
        info.upstreams.insert(up_node);
    }
    return false;
}

bool DownstreamRule::Accept(Plan* plan, Unit* unit) {
    return unit->is_leaf();
}

bool DownstreamRule::Run(Plan* plan, Unit* unit) {
    Info& info = unit->get<Info>();
    info.downstreams.clear();

    std::vector<Unit*> users = unit->direct_users();
    for (size_t i = 0; i < users.size(); ++i) {
        Unit* down_node = users[i];
        Info::NodeSet& downstreams = down_node->get<Info>().downstreams;
        info.downstreams.insert(downstreams.begin(), downstreams.end());
        info.downstreams.insert(down_node);
    }
    return false;
}

}  // namespace

bool DataFlowAnalysis::Run(Plan* plan) {
    TopologicalDispatcher forward_dependency(false);
    forward_dependency.AddRule(new UpstreamRule());
    forward_dependency.Run(plan);

    TopologicalDispatcher reverse_dependency(true);
    reverse_dependency.AddRule(new DownstreamRule());
    reverse_dependency.Run(plan);

    DepthFirstDispatcher control_dispatcher(DepthFirstDispatcher::POST_ORDER);
    control_dispatcher.AddRule(new DataFlowAnalysisRule());
    control_dispatcher.Run(plan);

    return false;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
