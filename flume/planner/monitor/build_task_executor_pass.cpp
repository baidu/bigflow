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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//

#include <set>

#include "flume/planner/monitor/build_task_executor_pass.h"

#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

namespace {

class BuildTaskExecutorRule : public RuleDispatcher::Rule {
public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        if (unit->type() == Unit::PLAN || unit->type() == Unit::JOB) {
            return false;
        }

        return !unit->is_leaf();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        PbExecutor& message = unit->get<PbExecutor>();
        if (unit->type() == Unit::TASK || unit->type() == Unit::SCOPE) {
            message.set_type(PbExecutor::TASK);
        }

        message.clear_child();
        for (Unit::iterator ptr = unit->begin(); ptr != unit->end(); ++ptr) {
            Unit* child = *ptr;
            if (child->is_leaf()) {
                continue;
            }

            CHECK(child->has<PbExecutor>());
            *message.add_child() = child->get<PbExecutor>();
        }

        return false;
    }
};

} // namespace

BuildTaskExecutorPass::~BuildTaskExecutorPass() {}

bool BuildTaskExecutorPass::Run(Plan* plan) {
    DepthFirstDispatcher dispatcher(DepthFirstDispatcher::POST_ORDER);
    dispatcher.AddRule(new BuildTaskExecutorRule());
    return dispatcher.Run(plan);
}

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu
