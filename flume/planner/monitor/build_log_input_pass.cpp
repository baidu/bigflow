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

#include "flume/planner/monitor/build_log_input_pass.h"

#include <vector>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"

#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/monitor/monitor_tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

namespace {

class BuildLogInputRule : public RuleDispatcher::Rule {
public:
    BuildLogInputRule() {}

    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::LOAD_NODE;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        Unit* father = unit->father();
        unit->set<ExecutedByFather>();
        father->set_type(Unit::EXTERNAL_EXECUTOR);
        father->get<External>().type = External::LOG_INPUT;
        PbMonitorTask::Input& input = father->get<PbMonitorTask::Input>();
        input.set_id(unit->get<PbLogicalPlanNode>().id());
        DrawPlanPass::UpdateLabel(father, "10-external-type",
                    External::TypeString(father->get<External>().type));

        PbExecutor* executor = &father->get<PbExecutor>();
        executor->set_type(PbExecutor::EXTERNAL);
        executor->mutable_external_executor()->set_id(input.id());

        return false;
    }
};

} // namespace

BuildLogInputPass::~BuildLogInputPass() {}

bool BuildLogInputPass::Run(Plan* plan) {
    DepthFirstDispatcher dispatcher(DepthFirstDispatcher::POST_ORDER);
    dispatcher.AddRule(new BuildLogInputRule());
    bool change = dispatcher.Run(plan);

    return change;
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu


