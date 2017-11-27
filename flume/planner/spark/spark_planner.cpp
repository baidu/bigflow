/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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
// Author: Wang Cong <bigflow-opensource@baidu.com>
//

#include "flume/planner/spark/spark_planner.h"
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
#include "flume/planner/common/remove_empty_unit_pass.h"
#include "flume/planner/common/remove_unsinked_pass.h"
#include "flume/planner/common/remove_useless_union_pass.h"
#include "flume/planner/common/scope_analysis.h"
#include "flume/planner/common/session_analysis.h"
#include "flume/planner/common/split_union_unit_pass.h"
#include "flume/planner/common/broadcast_shuffle_node_under_scope_analysis.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/spark/add_distribute_by_default_pass.h"
#include "flume/planner/spark/add_cache_task_pass.h"
#include "flume/planner/spark/add_task_unit_pass.h"
#include "flume/planner/spark/add_transfer_executor_pass.h"
#include "flume/planner/spark/build_physical_plan_pass.h"
#include "flume/planner/spark/build_transfer_executor_pass.h"
#include "flume/planner/spark/merge_task_pass.h"
#include "flume/planner/spark/set_default_concurrency_pass.h"
#include "flume/planner/spark/prune_cached_path_pass.h"
#include "flume/planner/unit.h"
#include "flume/runtime/resource.h"
#include "boost/lexical_cast.hpp"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class LogicalOptimizing : public AddTaskUnitPass {
public:
    RELY_PASS(LoadLogicalPlanPass);
    RELY_PASS(SplitUnionUnitPass);
    RELY_PASS(PromotePartialProcessPass);
    RELY_PASS(spark::PruneCachedPathPass);
    RELY_PASS(ScopeAnalysis)
    RELY_PASS(RemoveUnsinkedPass);
    RELY_PASS(RemoveUselessUnionPass);
    RELY_PASS(RemoveEmptyUnitPass);
    RELY_PASS(AddDistributeByDefaultPass);
    RELY_PASS(BroadcastShuffleNodeUnderScopeAnalysis);
    // APPLY_PASS(BroadcastShuffleNodeUnderScopeAnalysis);
private:
    virtual bool Run(Plan* plan) {
        bool changed = AddTaskUnitPass::Run(plan);
        plan->Root()->set<OptimizingStage>(RUNTIME_OPTIMIZING);
        return changed;
    }
};

class TopologicalOptimizing : public Pass {
public:
    RELY_PASS(MergeTaskPass);
    APPLY_PASS(SetDefaultConcurrencyPass);
private:
    virtual bool Run(Plan* plan) {
        plan->Root()->set<OptimizingStage>(RUNTIME_OPTIMIZING);
        return false;
    }
};

class RuntimeOptimizing : public Pass {
public:
    RELY_PASS(RemoveEmptyUnitPass);
    RELY_PASS(AddCommonExecutorPass);
    RELY_PASS(AddCacheTaskPass);
    RELY_PASS(AddTransferExecutorPass);

private:
    virtual bool Run(Plan* plan) {
        plan->Root()->set<OptimizingStage>(TRANSLATION_OPTIMIZING);
        return false;
    }
};

class TranslationOptimizing : public Pass {
public:
    RELY_PASS(BuildCommonExecutorPass);
    RELY_PASS(BuildTransferExecutorPass);
    RELY_PASS(BuildPhysicalPlanPass);
    PRESERVE_BY_DEFAULT();

private:
    virtual bool Run(Plan* plan) {
        // for draw plan
        return true;
    }
};

namespace {

class SparkPlannerImpl {
public:
    explicit SparkPlannerImpl(const PbLogicalPlan &message,
                              runtime::Resource::Entry* entry,
                              runtime::Session *session,
                              PbJobConfig *job_config);
    PbPhysicalPlan BuildPlan();

private:
    void DumpDebugFigure(runtime::Resource::Entry* entry, const std::string& content);
    std::string DebugString();
    void AppendToString(Unit* unit, std::string* text, std::string prefix);

private:
    Plan _plan;

    LoadLogicalPlanPass _logical_plan_loader;
    int _round;
    DrawPlanPass _drawer;
};

SparkPlannerImpl::SparkPlannerImpl(const PbLogicalPlan &message,
                                   runtime::Resource::Entry* entry,
                                   runtime::Session *session,
                                   PbJobConfig *job_config) {
    _plan.Root()->set<OptimizingStage>(LOGICAL_OPTIMIZING);
    _plan.Root()->get<JobConfig>().Assign(job_config);
    _plan.Root()->get<planner::Session>().Assign(session);

    _logical_plan_loader.Initialize(message);

    _round = 0;
    _drawer.RegisterListener(toft::NewPermanentClosure(
                this,
                &SparkPlannerImpl::DumpDebugFigure,
                entry));
}

void SparkPlannerImpl::DumpDebugFigure(runtime::Resource::Entry* entry,
                                       const std::string& content) {
    if (entry == NULL) {
        return;
    }

    std::ostringstream stream;
    stream << std::setfill('0') << std::setw(3) << _round++ << ".dot";
    entry->AddNormalFileFromBytes(stream.str(), content.data(), content.size());
}

PbPhysicalPlan SparkPlannerImpl::BuildPlan() {
    LOG(INFO) << "SparkPlanner start optimizing";
    runtime::Session tmp;
    _plan.Root()->get<NewSession>().Assign(&tmp);

    PassManager pass_manager(&_plan);
    pass_manager.RegisterPass(&_logical_plan_loader);
    pass_manager.SetDebugPass(&_drawer);

    pass_manager.Apply<LogicalOptimizing>();
    pass_manager.Apply<TopologicalOptimizing>();
    pass_manager.Apply<RuntimeOptimizing>();
    pass_manager.Apply<TranslationOptimizing>();

    if (_plan.Root()->has<Session>() && !_plan.Root()->get<Session>().is_null()) {
        *_plan.Root()->get<Session>() = *_plan.Root()->get<NewSession>();
    }

    LOG(INFO) << "SparkPlanner finished optimizing";
    return _plan.Root()->get<PbPhysicalPlan>();
}

std::string SparkPlannerImpl::DebugString() {
    Unit* root = _plan.Root();
    std::string text;
    AppendToString(root, &text, "");
    return text;
}

void SparkPlannerImpl::AppendToString(Unit* unit, std::string* text, std::string prefix) {
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

void SparkPlanner::SetDebugDirectory(runtime::Resource::Entry* entry) {
    _entry = entry;
}

SparkPlanner::SparkPlanner(runtime::Session *session, PbJobConfig *job_config)
    : _session(session), _job_config(job_config), _entry(NULL) { }

PbPhysicalPlan SparkPlanner::Plan(const PbLogicalPlan& message) {
    if (_entry != NULL) {
        std::string debug_string = message.DebugString();
        _entry->AddNormalFileFromBytes("logical_plan.debug",
                                        debug_string.data(), debug_string.size());
    }

    SparkPlannerImpl impl(message, _entry, _session, _job_config);
    PbPhysicalPlan result = impl.BuildPlan();
    if (_entry != NULL) {
        std::string debug_string = result.DebugString();
        _entry->AddNormalFileFromBytes("physical_plan.debug",
                                        debug_string.data(), debug_string.size());
    }
    return result;
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu
