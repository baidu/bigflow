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

#include "flume/planner/monitor/monitor_planner.h"

#include <iomanip>
#include <set>
#include <sstream>
#include <string>

#include "boost/lexical_cast.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "toft/base/closure.h"
#include "toft/crypto/uuid/uuid.h"

#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/load_logical_plan_pass.h"
#include "flume/planner/monitor/add_channel_unit_pass.h"
#include "flume/planner/monitor/add_fixed_task_unit_pass.h"
#include "flume/planner/monitor/build_log_input_pass.h"
#include "flume/planner/monitor/build_physical_plan_pass.h"
#include "flume/planner/monitor/build_reader_writer_pass.h"
#include "flume/planner/pass_manager.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

class MonitorPlannerImpl {
public:
    MonitorPlannerImpl(const PbLogicalPlan& message,
                    runtime::Resource::Entry* entry);

    PbPhysicalPlan BuildPlan();

private:
    void DumpDebugFigure(runtime::Resource::Entry* entry, const std::string& content);

private:
    Plan m_plan;
    LoadLogicalPlanPass m_loader;
    int m_round;
    DrawPlanPass m_drawer;
};

MonitorPlannerImpl::MonitorPlannerImpl(const PbLogicalPlan& message,
                                    runtime::Resource::Entry* entry) {
    m_loader.Initialize(message);
    m_round = 0;
    m_drawer.RegisterListener(toft::NewPermanentClosure(this, &MonitorPlannerImpl::DumpDebugFigure,
                                                        entry));
}

void MonitorPlannerImpl::DumpDebugFigure(runtime::Resource::Entry* entry,
                                        const std::string& content) {
    if (entry == NULL) {
        return;
    }
    std::ostringstream stream;
    stream << std::setfill('0') << std::setw(3) << m_round++ << ".dot";
    entry->AddNormalFileFromBytes(stream.str(), content.data(), content.size());
}

PbPhysicalPlan MonitorPlannerImpl::BuildPlan() {
    PassManager pm(&m_plan);
    pm.RegisterPass(&m_loader);
    pm.SetDebugPass(&m_drawer);
    pm.Apply<LoadLogicalPlanPass>();
    pm.Apply<AddFixedTaskUnitPass>();

    pm.Apply<BuildLogInputPass>();
    pm.Apply<BuildReaderWriterPass>();

    pm.Apply<AddChannelUnitPass>();

    pm.Apply<BuildPhysicalPlanPass>();
    // TODO(Pan Yuchang)
    return m_plan.Root()->get<PbPhysicalPlan>();
}

MonitorPlanner::MonitorPlanner() : m_entry(NULL) {}

void MonitorPlanner::SetDebugDirectory(runtime::Resource::Entry* entry) {
    m_entry = entry;
}

PbPhysicalPlan MonitorPlanner::Plan(const PbLogicalPlan& message) {
    if (m_entry != NULL) {
        std::string debug_string = message.DebugString();
        m_entry->AddNormalFileFromBytes("logical_plan.debug",
                                        debug_string.data(), debug_string.size());
    }

    MonitorPlannerImpl impl(message, m_entry);
    PbPhysicalPlan result = impl.BuildPlan();
    if (m_entry != NULL) {
        std::string debug_string = result.DebugString();
        m_entry->AddNormalFileFromBytes("physical_plan.debug",
                                        debug_string.data(), debug_string.size());
    }

    return result;
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu

