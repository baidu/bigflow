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
// Author: Pan Yuchang(BDG)<panyuchang@baidu.com>
// Description:
//   This pass should run after BuildDecoderPass, and is used to add CHANNEL node between
// reader node and decoder node, only handle one input and one output situation by following
// steps:
//      1. Analysis local shuffle node and merge shuffle node.
//      2. Analysis initial nodes which need to add CHANNEL.
//      3. Add CHANNEL from leaf node to root node.

#include "flume/planner/monitor/add_channel_unit_pass.h"

#include <set>
#include <vector>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"

#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/monitor/monitor_tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

namespace {

struct ChannelNodeSet {
    std::set<Unit*> channels;
};

class LocalShuffleAnalysisRule : public RuleDispatcher::Rule {
public:
    LocalShuffleAnalysisRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::SHUFFLE_NODE;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* father = unit->father();
        father->set_type(Unit::SHUFFLE_EXECUTOR);
        father->get<PbShuffleNode::Type>() = PbShuffleNode::KEY;

        PbLogicalPlanNode& node = unit->get<PbLogicalPlanNode>();
        node.mutable_shuffle_node()->set_type(PbShuffleNode::KEY);
        return false;
    }
};

class MergeShuffleAnalysisRule : public RuleDispatcher::Rule {
public:
    MergeShuffleAnalysisRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        DataFlow& info = unit->get<DataFlow>();
        return info.inputs.size() != 0 && unit->type() == Unit::SCOPE;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        unit->set_type(Unit::SHUFFLE_EXECUTOR);
        unit->get<PbShuffleNode::Type>() = PbShuffleNode::KEY;
        return false;
    }
};

class FindChannelUnitRule : public RuleDispatcher::Rule {
public:
    FindChannelUnitRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::CHANNEL
                && unit->father()->type() == Unit::EXTERNAL_EXECUTOR
                && unit->father()->get<External>().type == External::MONITOR_DECODER;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* father = unit->father();
        std::set<Unit*>* channels = &father->get<ChannelNodeSet>().channels;
        channels->insert(unit);
        return false;
    }
};

} // namespace

AddChannelUnitPass::~AddChannelUnitPass() {}

bool AddChannelUnitPass::Run(Plan* plan) {
    DepthFirstDispatcher l_shuffle_dispatcher(DepthFirstDispatcher::PRE_ORDER);
    l_shuffle_dispatcher.AddRule(new LocalShuffleAnalysisRule());
    bool change = l_shuffle_dispatcher.Run(plan);

    DepthFirstDispatcher m_shuffle_dispatcher(DepthFirstDispatcher::PRE_ORDER);
    m_shuffle_dispatcher.AddRule(new MergeShuffleAnalysisRule());
    change |= m_shuffle_dispatcher.Run(plan);

    DepthFirstDispatcher find_dispatcher(DepthFirstDispatcher::PRE_ORDER);
    find_dispatcher.AddRule(new FindChannelUnitRule());
    change |= find_dispatcher.Run(plan);

    return change;
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu


