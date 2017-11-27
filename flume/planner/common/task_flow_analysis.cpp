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
// Author: Pan Yuchang(BDG)<bigflow-opensource@baidu.com>
// Description:

#include "flume/planner/common/task_flow_analysis.h"

#include <algorithm>

#include "boost/foreach.hpp"
#include "glog/logging.h"

#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

typedef DataFlowAnalysis::Info DataInfo;
typedef TaskFlowAnalysis::Info TaskInfo;

class TaskFlowAnalysisRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit);
    virtual bool Run(Plan* plan, Unit* unit);
};

bool TaskFlowAnalysisRule::Accept(Plan* plan, Unit* unit) {
    return unit->type() == Unit::TASK;
}

bool TaskFlowAnalysisRule::Run(Plan* plan, Unit* unit) {
    DataInfo& data_info = unit->get<DataInfo>();
    TaskInfo& task_info = unit->get<TaskInfo>();
    task_info.input_tasks.clear();
    BOOST_FOREACH(Unit* need, data_info.needs) {
        Unit* task = need->task();
        task_info.input_tasks.insert(task);
    }
    task_info.output_tasks.clear();
    BOOST_FOREACH(Unit* user, data_info.users) {
        Unit* task = user->task();
        task_info.output_tasks.insert(task);
    }
    return false;
}

} // namespace

bool TaskFlowAnalysis::Run(Plan* plan) {
    DepthFirstDispatcher task_control_dispatcher(DepthFirstDispatcher::PRE_ORDER);
    task_control_dispatcher.AddRule(new TaskFlowAnalysisRule());
    task_control_dispatcher.Run(plan);

    return false;
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
