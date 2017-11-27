/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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

#ifndef FLUME_PLANNER_LOCAL_ADD_TASK_UNIT_PASS_H_
#define FLUME_PLANNER_LOCAL_ADD_TASK_UNIT_PASS_H_

#include "flume/planner/plan.h"
#include "flume/planner/unit.h"
#include "flume/planner/rule_dispatcher.h"

namespace baidu {
namespace flume {
namespace planner {
namespace local {

class AddTaskUnitPass : public Pass {
    PRESERVE_BY_DEFAULT();
    HOLD_BY_DEFAULT();
public:
    virtual ~AddTaskUnitPass() {}

    virtual bool Run(Plan* plan) {
        RuleDispatcher dispatcher;
        dispatcher.AddRule(new AddTasks);
        plan->Root()->set_type(Unit::TASK);
        return dispatcher.Run(plan);
    }

    class AddTasks : public RuleDispatcher::Rule {
    public:
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->father() == plan->Root() && unit->type() > Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            if (unit->type() == Unit::SCOPE) {
                const PbScope& scope = unit->get<PbScope>();
                if (scope.type() == PbScope::BUCKET
                        && !scope.has_concurrency()
                        && !unit->has<IsDistributeAsBatch>()) {
                    unit->set<IsLocalDistribute>();
                }
            }
            return true;
        }
    };
};

}  // namespace local
}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif
