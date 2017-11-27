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
//
// Author: Wen Xiang <wenxiang@baidu.com>
// Modified: daiweiwei01@baidu.com
//
// Planner for execution in localhost only.

#include "flume/planner/local/local_planner.h"
#include <iomanip>
#include <sstream>
#include <string>

#include "flume/planner/common/add_common_executor_pass.h"
#include "flume/planner/common/build_common_executor_pass.h"
#include "flume/planner/common/cache_analysis.h"
#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/load_logical_plan_pass.h"
#include "flume/planner/common/promote_partial_process_pass.h"
#include "flume/planner/common/prune_cached_path_pass.h"
#include "flume/planner/common/remove_empty_unit_pass.h"
#include "flume/planner/common/remove_unsinked_pass.h"
#include "flume/planner/common/remove_useless_union_pass.h"
#include "flume/planner/common/scope_analysis.h"
#include "flume/planner/common/session_analysis.h"
#include "flume/planner/common/split_union_unit_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/local/add_task_unit_pass.h"
#include "flume/planner/local/build_local_input_executor_pass.h"
#include "flume/planner/local/build_physical_plan_pass.h"
#include "flume/planner/unit.h"
#include "flume/runtime/resource.h"
#include "boost/lexical_cast.hpp"

namespace baidu {
namespace flume {
namespace planner {
namespace local {

class LogicalOptimizing : public AddTaskUnitPass {
public:
    RELY_PASS(LoadLogicalPlanPass);
    RELY_PASS(SplitUnionUnitPass);
    RELY_PASS(PromotePartialProcessPass);
    RELY_PASS(PruneCachedPathPass);
    RELY_PASS(ScopeAnalysis)
    RELY_PASS(RemoveUnsinkedPass);
    RELY_PASS(RemoveUselessUnionPass);
    RELY_PASS(RemoveEmptyUnitPass);
private:
    virtual bool Run(Plan* plan) {
        bool changed = AddTaskUnitPass::Run(plan);
        plan->Root()->set<OptimizingStage>(RUNTIME_OPTIMIZING);
        return changed;
    }
};

class RuntimeOptimizing : public Pass {
public:
    RELY_PASS(RemoveEmptyUnitPass);
    RELY_PASS(AddCommonExecutorPass);

private:
    virtual bool Run(Plan* plan) {
        plan->Root()->set<OptimizingStage>(TRANSLATION_OPTIMIZING);
        return false;
    }
};

class TranslationOptimizing : public Pass {
public:
    RELY_PASS(BuildCommonExecutorPass);
    RELY_PASS(BuildLocalInputExecutorPass);
    RELY_PASS(BuildPhysicalPlanPass);
    PRESERVE_BY_DEFAULT();

private:
    virtual bool Run(Plan* plan) {
        // for draw plan
        return true;
    }
};

namespace {

class LocalPlannerImpl {
public:
    explicit LocalPlannerImpl(const PbLogicalPlan &message,
                              runtime::Resource::Entry* entry,
                              runtime::Session *session,
                              PbJobConfig *job_config);
    PbPhysicalPlan BuildPlan();

private:
    void DumpDebugFigure(runtime::Resource::Entry* entry, const std::string& content);
    std::string DebugString();
    void AppendToString(Unit* unit, std::string* text, std::string prefix);

private:
    Plan m_plan;

    LoadLogicalPlanPass m_logical_plan_loader;
    int m_round;
    DrawPlanPass m_drawer;
};

LocalPlannerImpl::LocalPlannerImpl(const PbLogicalPlan &message,
                                   runtime::Resource::Entry* entry,
                                   runtime::Session *session,
                                   PbJobConfig *job_config) {
    m_plan.Root()->set<OptimizingStage>(LOGICAL_OPTIMIZING);
    m_plan.Root()->get<JobConfig>().Assign(job_config);
    m_plan.Root()->get<planner::Session>().Assign(session);

    m_logical_plan_loader.Initialize(message);

    m_round = 0;
    m_drawer.RegisterListener(toft::NewPermanentClosure(this, &LocalPlannerImpl::DumpDebugFigure,
                                                        entry));
}

void LocalPlannerImpl::DumpDebugFigure(runtime::Resource::Entry* entry,
                                       const std::string& content) {
    if (entry == NULL) {
        return;
    }

    std::ostringstream stream;
    stream << std::setfill('0') << std::setw(3) << m_round++ << ".dot";
    entry->AddNormalFileFromBytes(stream.str(), content.data(), content.size());
}

PbPhysicalPlan LocalPlannerImpl::BuildPlan() {
    LOG(INFO) << "LocalPlanner start optimizing";
    runtime::Session tmp;
    m_plan.Root()->get<NewSession>().Assign(&tmp);

    PassManager pass_manager(&m_plan);
    pass_manager.RegisterPass(&m_logical_plan_loader);
    pass_manager.SetDebugPass(&m_drawer);

    pass_manager.Apply<LogicalOptimizing>();
    pass_manager.Apply<RuntimeOptimizing>();
    pass_manager.Apply<TranslationOptimizing>();

    if (m_plan.Root()->has<Session>() && !m_plan.Root()->get<Session>().is_null()) {
        *m_plan.Root()->get<Session>() = *m_plan.Root()->get<NewSession>();
    }

    LOG(INFO) << "LocalPlanner finished optimizing";
    return m_plan.Root()->get<PbPhysicalPlan>();
}

std::string LocalPlannerImpl::DebugString() {
    Unit* root = m_plan.Root();
    std::string text;
    AppendToString(root, &text, "");
    return text;
}

void LocalPlannerImpl::AppendToString(Unit* unit, std::string* text, std::string prefix) {
    if (unit->type() != Unit::JOB) {
        DataFlowAnalysis::Info& info = unit->get<DataFlowAnalysis::Info>();
        *text += "\n" + prefix + unit->type_string() + ": " + unit->identity();
        *text += ", users " + boost::lexical_cast<std::string>(info.users.size());
        *text += ", needs " + boost::lexical_cast<std::string>(info.needs.size());
        *text += ", total " + boost::lexical_cast<std::string>(info.nodes.size());
        prefix += "    ";
        if (info.users.size() == 1) {
            *text += "\n" + prefix + (*info.users.begin())->identity();
        }
    }
    for (Unit::iterator it = unit->begin(); it != unit->end(); ++it) {
        AppendToString(*it, text, prefix);
    }
}

}  // anonymous namespace

void LocalPlanner::SetDebugDirectory(runtime::Resource::Entry* entry) {
    m_entry = entry;
}

LocalPlanner::LocalPlanner(runtime::Session *session, PbJobConfig *job_config)
    : m_session(session), m_job_config(job_config), m_entry(NULL) { }

PbPhysicalPlan LocalPlanner::Plan(const PbLogicalPlan& message) {
    if (m_entry != NULL) {
        std::string debug_string = message.DebugString();
        m_entry->AddNormalFileFromBytes("logical_plan.debug",
                                        debug_string.data(), debug_string.size());
    }

    LocalPlannerImpl impl(message, m_entry, m_session, m_job_config);
    PbPhysicalPlan result = impl.BuildPlan();
    if (m_entry != NULL) {
        std::string debug_string = result.DebugString();
        m_entry->AddNormalFileFromBytes("physical_plan.debug",
                                        debug_string.data(), debug_string.size());
    }
    return result;
}

}  // namespace local
}  // namespace planner
}  // namespace flume
}  // namespace baidu
