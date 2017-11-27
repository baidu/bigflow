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
//         Wen Xiang <wenxiang@baidu.com>

#include "flume/planner/common/scope_analysis.h"

#include "boost/foreach.hpp"

#include "flume/planner/common/tags.h"
#include "flume/planner/depth_first_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/rule_dispatcher.h"

namespace baidu {
namespace flume {
namespace planner {

class ScopeAnalysis::Impl {
public:
    static bool Run(Plan* plan) {
        DepthFirstDispatcher update_phase(DepthFirstDispatcher::PRE_ORDER);
        update_phase.AddRule(new UpdateKeyScopesForRoot);
        update_phase.AddRule(new UpdateKeyScopesForScopeUnit);
        update_phase.AddRule(new UpdateKeyScopesForLeafUnit);
        update_phase.AddRule(new SetHasPartitioner);
        update_phase.AddRule(new SetIsDistributeAsBatch);
        update_phase.Run(plan);

        return false;
    }

    class UpdateKeyScopesForRoot : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit == plan->Root() || unit->type() == Unit::TASK;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->get<KeyScopes>().clear();
            return false;
        }
    };

    class UpdateKeyScopesForScopeUnit : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->type() == Unit::SCOPE && unit->father() != NULL;
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            KeyScopes& scopes = unit->get<KeyScopes>();
            scopes = unit->father()->get<KeyScopes>();
            scopes.push_back(unit->father()->get<PbScope>());
            return false;
        }
    };

    class UpdateKeyScopesForLeafUnit : public RuleDispatcher::Rule {
        virtual bool Accept(Plan* plan, Unit* unit) {
            return unit->is_leaf();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            KeyScopes& scopes = unit->get<KeyScopes>();
            scopes = unit->father()->get<KeyScopes>();
            scopes.push_back(unit->father()->get<PbScope>());
            unit->get<ScopeLevel>().level = scopes.size() - 1;
            return false;
        }
    };

    class SetHasPartitioner : public RuleDispatcher::Rule {
        bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::SHUFFLE_NODE) {
                return false;
            }
            CHECK(unit->has<PbLogicalPlanNode>());
            return unit->get<PbLogicalPlanNode>().shuffle_node().has_partitioner();
        }

        bool Run(Plan* plan, Unit* unit) {
            unit->father()->get<HasPartitioner>();
            return false;
        }
    };

    class SetIsDistributeAsBatch : public RuleDispatcher::Rule {
        bool Accept(Plan* plan, Unit* unit) {
            if (unit->type() != Unit::SCOPE || unit->father() == NULL) {
                return false;
            }
            CHECK(unit->has<PbScope>());
            const PbScope& scope = unit->get<PbScope>();
            return scope.type() == PbScope::BUCKET && scope.is_infinite() && !scope.is_stream();
        }

        virtual bool Run(Plan* plan, Unit* unit) {
            unit->get<IsDistributeAsBatch>();
            return false;
        }
    };
};

bool ScopeAnalysis::Run(Plan* plan) {
    return Impl::Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu

