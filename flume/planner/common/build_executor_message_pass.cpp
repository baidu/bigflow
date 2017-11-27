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
//         Wen Xiang <bigflow-opensource@baidu.com>

#include "flume/planner/common/build_executor_message_pass.h"

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

class BuildExecutorMessagePass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher init_phase;
        init_phase.AddRule(new CleanTags);
        init_phase.Run(plan);

        TopologicalDispatcher analyze_phase(TopologicalDispatcher::REVERSE_ORDER);
        analyze_phase.AddRule(new AnalyzeUsageCount);
        analyze_phase.AddRule(new AnalyzeNeedDatasetForPrepareUnit);
        analyze_phase.AddRule(new AnalyzeNeedDatasetForShuffleNode);
        analyze_phase.Run(plan);

        RuleDispatcher build_phase;
        build_phase.AddRule(new AnalyzeInputOutput);
        build_phase.AddRule(new UpdateExecutorMessage);
        build_phase.Run(plan);

        return false;
    }

    struct UsageCount : Value<int> {};

    struct NeedDataset {};

    struct Inputs : std::set<Unit*> {};

    struct Outputs : std::set<Unit*> {};

    class CleanTags : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<UsageCount>();
            unit->clear<NeedDataset>();
            unit->clear<Inputs>();
            unit->clear<Outputs>();
            return false;
        }
    };

    class AnalyzeUsageCount : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            BOOST_FOREACH(Unit* need, unit->direct_needs()) {
                if (need->task() != unit->task()) {
                    continue;
                }

                if (need->father() != unit->father()) {
                    *need->get<UsageCount>() += 1;
                }
            }

            return false;
        }
    };

    class AnalyzeNeedDatasetForPrepareUnit : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->is_leaf() && unit->has<NeedPrepare>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<NeedDataset>();
            DrawPlanPass::UpdateLabel(unit, "30-need-dataset", "NeedDataset");
            return false;
        }
    };

    class AnalyzeNeedDatasetForShuffleNode : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::SHUFFLE_NODE
                    && unit->father()->type() == Unit::SHUFFLE_EXECUTOR;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set<NeedDataset>();
            DrawPlanPass::UpdateLabel(unit, "30-need-dataset", "NeedDataset");
            return false;
        }
    };

    class AnalyzeInputOutput : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return !unit->is_leaf() && unit->type() >= Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            typedef DataFlow::Dependency Dependency;
            DataFlow& dataflow = unit->get<DataFlow>();

            BOOST_FOREACH(Dependency dep, dataflow.inputs) {
                if (dep.first->task() == unit->task()) {
                    unit->get<Inputs>().insert(dep.first);
                }
            }

            BOOST_FOREACH(Dependency dep, dataflow.outputs) {
                if (dep.second->task() == unit->task()) {
                    unit->get<Outputs>().insert(dep.first);
                }
            }

            return false;
        }
    };

    class UpdateExecutorMessage : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return !unit->is_leaf() && unit->type() >= Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            PbExecutor* message = &unit->get<PbExecutor>();

            message->clear_input();
            BOOST_FOREACH(Unit* input, unit->get<Inputs>()) {
                message->add_input(input->identity());
            }

            message->clear_output();
            BOOST_FOREACH(Unit* output, unit->get<Outputs>()) {
                message->add_output(output->identity());
            }

            message->clear_dispatcher();
            BOOST_FOREACH(Unit* child, unit->children()) {
                //if (!child->is_leaf() || *child->get<UsageCount>() == 0) {
                if (!child->is_leaf() || child->type() == Unit::SINK_NODE) {
                    continue;
                }

                PbExecutor::Dispatcher* dispatcher = message->add_dispatcher();
                dispatcher->set_identity(child->identity());
                if (child->has<PbLogicalPlanNode>()) {
                    *dispatcher->mutable_objector() = child->get<PbLogicalPlanNode>().objector();
                }
                if (child->has<PreparePriority>()) {
                    dispatcher->set_priority(*child->get<PreparePriority>());
                }
                dispatcher->set_usage_count(*child->get<UsageCount>());
                dispatcher->set_need_dataset(child->has<NeedDataset>());
            }

            return false;
        }
    };
};

bool BuildExecutorMessagePass::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
