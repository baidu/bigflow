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
// Author: Zhou Kai <zhoukai01@baidu.com>
//         Wen Xiang <wenxiang@baidu.com>

#include "flume/planner/common/scope_level_analysis.h"

#include "boost/lexical_cast.hpp"

#include "flume/planner/common/draw_plan_pass.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

class ComputeScopeLevelRule : public RuleDispatcher::Rule {
public:
    struct Info {
        std::vector<std::string> scopes;
    };

public:
    virtual bool Accept(Plan* plan, Unit* unit) {
        if (unit->type() < Unit::TASK) {
            return false;
        }

        return !unit->is_leaf();
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        // update scopes
        std::vector<std::string>& scopes = unit->get<Info>().scopes;
        if (unit->type() == Unit::TASK) {
            scopes.clear();
        } else {
            scopes = unit->father()->get<Info>().scopes;
        }

        if (unit->has<PbScope>() && unit->get<PbScope>().id() != "") {
            const PbScope& scope = unit->get<PbScope>();
            if (scopes.empty() || scope.id() != scopes.back()) {
                scopes.push_back(scope.id());
            }
        }

        // update scope_level
        PbExecutor& message = unit->get<PbExecutor>();
        if (message.has_scope_level() && message.scope_level() == scopes.size()) {
            return false;
        }

        message.set_scope_level(scopes.size());
        DrawPlanPass::AddMessage(unit,
                                 "Update scope level to " +
                                 boost::lexical_cast<std::string>(scopes.size()));
        return true;
    }
};

}  // namespace

bool ScopeLevelAnalysis::Run(Plan* plan) {
    DepthFirstDispatcher dispatcher(DepthFirstDispatcher::PRE_ORDER);
    dispatcher.AddRule(new ComputeScopeLevelRule());
    return dispatcher.Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
