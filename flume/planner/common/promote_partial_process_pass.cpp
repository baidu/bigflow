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

#include "flume/planner/common/promote_partial_process_pass.h"

#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

struct SortedSource : public Pointer<Unit> {};

class PromotePartialProcessPass::PromoteProcessPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher clear_phase;
        clear_phase.AddRule(new ClearTags);
        clear_phase.Run(plan);

        DepthFirstDispatcher analysis_phase(DepthFirstDispatcher::PRE_ORDER);
        analysis_phase.AddRule(new SortedSourceAnalysis);
        analysis_phase.AddRule(new KeyNumberAnalysis);
        analysis_phase.Run(plan);

        RuleDispatcher promote_dispatcher;
        promote_dispatcher.AddRule(new FindPartialProcessAndPromote);
        return promote_dispatcher.Run(plan);
    }

    class ClearTags : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return true;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->clear<SortedSource>();
            unit->clear<PartialKeyNumber>();
            return false;
        }
    };

    class SortedSourceAnalysis : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit != plan->Root();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* father = unit->father();
            if (!unit->is_leaf() && unit->get<PbScope>().is_sorted()) {
                if (father == plan->Root()) {
                    unit->get<SortedSource>().Assign(unit);
                }
                if (father->has<SortedSource>() && father->get<SortedSource>().get() == father) {
                    unit->get<SortedSource>().Assign(unit);
                }
            }
            if (father->has<SortedSource>() && !unit->has<SortedSource>()) {
                unit->get<SortedSource>().Assign(father->get<SortedSource>().get());
            }

            return false;
        }
    };

    class KeyNumberAnalysis : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit != plan->Root();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* father = unit->father();
            if (father == plan->Root()) {
                *unit->get<PartialKeyNumber>() = 1;
            } else if (father->has<PartialKeyNumber>()) {
                *unit->get<PartialKeyNumber>() = *father->get<PartialKeyNumber>() + 1;
            }
            return false;
        }
    };

    class FindPartialProcessAndPromote : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::PROCESS_NODE || !IsPartial(unit)) {
                return false;
            }
            if (unit->direct_needs().size() != 1) {
                return false;
            }
            Unit* need = unit->direct_needs()[0];
            if (need->father() == unit->father() || !need->is_descendant_of(unit->father())) {
                return false;
            }
            return need->has<SortedSource>();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* need = unit->direct_needs()[0];
            Unit* new_father = need->get<SortedSource>().get();
            plan->RemoveControl(unit->father(), unit);
            plan->AddControl(new_father, unit);
            return true;
        }

    private:
        bool IsPartial(Unit* unit) {
            CHECK(unit->has<PbLogicalPlanNode>());
            const PbProcessNode& process_node = unit->get<PbLogicalPlanNode>().process_node();
            for (int i = 0; i < process_node.input_size(); ++i) {
                if (!process_node.input(i).is_partial()) {
                    return false;
                }
            }

            if (process_node.input_size() != 0) {
                return true;
            }

            return false;
        }
    };
};

bool PromotePartialProcessPass::PromoteProcessPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

