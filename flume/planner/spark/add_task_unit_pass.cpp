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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>
//         Wang Cong <wangcong09@baidu.com>
//

#include "flume/planner/spark/add_task_unit_pass.h"

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "toft/crypto/uuid/uuid.h"

#include "flume/flags.h"
#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/spark/tags.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class AddTaskUnitPass::Impl {
public:
    static bool Run(Plan* plan) {
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new ChangeRootToPlan);
        dispatcher.AddRule(new AddTasks);
        return dispatcher.Run(plan);
    }

    class ChangeRootToPlan : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit == plan->Root();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->set_type(Unit::PLAN);
            return true;
        }
    };

    class AddTasks : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->father() == plan->Root() && unit->type() > Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            Unit* task = plan->Root()->clone();
            task->set_identity(toft::CreateCanonicalUUIDString());
            task->set_type(Unit::TASK);
            plan->ReplaceControl(plan->Root(), unit, task);

            int concurrency = 0;
            if (unit->type() == Unit::SCOPE) {
                const PbScope& scope = unit->get<PbScope>();
                if (scope.type() == PbScope::BUCKET) {
                    unit->set<IsLocalDistribute>();
                } else if (scope.type() == PbScope::INPUT) {
                    task->set<IsHadoopInput>();
                }
                if (scope.has_concurrency()) {
                    concurrency = scope.concurrency();
                }
            } else {
                task->set<SinglePoint>();
                concurrency = 1;
            }
            if (concurrency > 0) {
                *task->get<TaskConcurrency>() = concurrency;

                std::string label = "concurrency: "
                        + boost::lexical_cast<std::string>(concurrency)
                        + (task->has<SinglePoint>() ? ", single_point" : "");
                DrawPlanPass::UpdateLabel(task, "10-concurrency", label);
            }
            return true;
        }
    };
};

bool AddTaskUnitPass::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu
