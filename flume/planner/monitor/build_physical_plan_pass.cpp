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

#include "flume/planner/monitor/build_physical_plan_pass.h"

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "toft/crypto/uuid/uuid.h"

#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/monitor/monitor_tags.h"
#include "flume/planner/monitor/prepared_node_basic_analysis.h"
#include "flume/planner/plan.h"
#include "flume/planner/task_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

namespace {

PbMonitorJob* JobMessageOf(Unit* root) {
    return root->get<PbPhysicalPlan>().mutable_job(0)->mutable_monitor_job();
}

PbMonitorTask* TaskMessageOf(Unit* unit) {
    return &unit->task()->get<PbMonitorTask>();
}

class InitializePlanRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit == plan->Root();
    }

    virtual bool Run(Plan* _, Unit* unit) {
        PbPhysicalPlan* plan = &unit->get<PbPhysicalPlan>();
        plan->clear_job();

        PbJob* job = plan->add_job();
        job->set_id(toft::CreateCanonicalUUIDString());
        job->set_type(PbJob::MONITOR);
        return false;
    }
};

class InitializeTaskRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::TASK;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        PbMonitorTask& task = unit->get<PbMonitorTask>();
        task.clear_input();
        task.clear_monitor_writer();
        task.clear_monitor_reader();
        *task.mutable_root() = unit->get<PbExecutor>();
        return false;
    }
};

template<External::Type N>
class ExternalRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::EXTERNAL_EXECUTOR && unit->get<External>().type == N;
    }

protected:
    template<typename T>
    T& CheckAndGet(Unit* unit) {
        CHECK(unit->has<T>());
        return unit->get<T>();
    }
};

class SetLogInputRule : public ExternalRule<External::LOG_INPUT> {
public:
    virtual bool Run(Plan* plan, Unit* unit) {
        *TaskMessageOf(unit)->mutable_input()
                = CheckAndGet<PbMonitorTask::Input>(unit);
        return false;
    }
};

class AddMonitorReaderRule : public ExternalRule<External::MONITOR_READER> {
public:
    virtual bool Run(Plan* plan, Unit* unit) {
        *TaskMessageOf(unit)->mutable_monitor_reader() =
                CheckAndGet<PbMonitorTask::MonitorReader>(unit);
        return false;
    }
};

class AddMonitorWriterRule : public ExternalRule<External::MONITOR_WRITER> {
public:
    virtual bool Run(Plan* plan, Unit* unit) {
        *TaskMessageOf(unit)->add_monitor_writer() =
                CheckAndGet<PbMonitorTask::MonitorWriter>(unit);
        return false;
    }
};

class AddPbScopeListRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() >= Unit::TASK && unit->has<PbScope>();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        *TaskMessageOf(unit)->add_scope()
                = unit->get<PbScope>();
        return false;
    }
};

class AddPbNodeListRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() >= Unit::TASK && unit->has<PbLogicalPlanNode>();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        *TaskMessageOf(unit)->add_node()
                = unit->get<PbLogicalPlanNode>();
        return false;
    }
};

class AddTaskMessageRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        return unit->type() == Unit::TASK;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        PbMonitorTask& task = unit->get<PbMonitorTask>();
        PbMonitorJob* job = JobMessageOf(plan->Root());
        TaskInformation::Type task_type = unit->get<TaskInformation>().type;
        switch (task_type) {
        case TaskInformation::WORKER_STREAM:
            *job->mutable_worker_stream_task() = task;
            break;
        case TaskInformation::WORKER_PREPARED:
            *job->mutable_worker_prepared_task() = task;
            break;
        case TaskInformation::CLIENT_STREAM:
            *job->mutable_client_stream_task() = task;
            break;
        case TaskInformation::CLIENT_PREPARED:
            *job->mutable_client_prepared_task() = task;
            break;
        default:
            LOG(FATAL) << "Unrecognize task type!";
        }
        return false;
    }
};
} // namespace

BuildPhysicalPlanPass::~BuildPhysicalPlanPass() {}

bool BuildPhysicalPlanPass::Run(Plan* plan) {
    DepthFirstDispatcher init_plan_round(DepthFirstDispatcher::PRE_ORDER);
    init_plan_round.AddRule(new InitializePlanRule());
    init_plan_round.Run(plan);

    TaskDispatcher init_task_round(TaskDispatcher::PRE_TOPOLOGY_ORDER);
    init_task_round.AddRule(new InitializeTaskRule());
    init_task_round.Run(plan);

    DepthFirstDispatcher first_round(DepthFirstDispatcher::PRE_ORDER);
    first_round.AddRule(new SetLogInputRule());
    first_round.AddRule(new AddMonitorReaderRule());
    first_round.AddRule(new AddMonitorWriterRule());
    first_round.AddRule(new AddPbNodeListRule());
    first_round.AddRule(new AddPbScopeListRule());
    first_round.Run(plan);

    TaskDispatcher add_task_round(TaskDispatcher::PRE_TOPOLOGY_ORDER);
    add_task_round.AddRule(new AddTaskMessageRule());
    add_task_round.Run(plan);

    return false;
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu

