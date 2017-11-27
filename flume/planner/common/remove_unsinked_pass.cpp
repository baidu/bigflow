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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>
//         Pan Yuchang <panyuchang@baidu.com>
#include "flume/planner/common/remove_unsinked_pass.h"

#include <algorithm>
#include <deque>
#include <vector>
#include <iostream>

#include "flume/planner/common/tags.h"
#include "flume/planner/rule_dispatcher.h"
#include "flume/planner/topological_dispatcher.h"
#include "flume/planner/unit.h"

#include "boost/foreach.hpp"
#include "toft/base/unordered_set.h"

namespace baidu {
namespace flume {
namespace planner {

namespace {

class RemoveUnsinkedPassRule : public RuleDispatcher::Rule {
public:
    virtual ~RemoveUnsinkedPassRule() {}

    bool HasProcessNodeInSameShuffleScope(Unit* unit) {
        if (unit->father()->type() != Unit::SCOPE
                && unit->father()->type() != Unit::SHUFFLE_EXECUTOR) {
            return false;
        }
        BOOST_FOREACH(Unit* brother, unit->father()->children()) {
            if (brother->type() == Unit::PROCESS_NODE) {
                return true;
            }
            if (brother->type() == Unit::LOGICAL_EXECUTOR) {
                CHECK_EQ(1, brother->children().size());
                if (brother->children()[0]->type() == Unit::PROCESS_NODE) {
                    return true;
                }
            }
        }
        return false;
    }

    virtual bool Accept(Plan* plan, Unit* unit) {
        if (unit->has<HasSideEffect>()) {
            return false;
        }

        if (unit->direct_users().size() != 0) {
            return false;
        }

        // TODO: Open this comments
        // if (unit->has<GroupGenerator>()) {
        //     // if unit is Group Generator
        //     // we do not remove it unless there is no ProcessNode in same scope
        //     return !HasProcessNodeInSameShuffleScope(unit);
        // }
        return true;
    }

    virtual bool Run(Plan* plan, Unit* unit) {
        plan->RemoveUnit(unit);
        return true;
    }
};

} // namespace

RemoveUnsinkedPass::~RemoveUnsinkedPass() {}

bool RemoveUnsinkedPass::Run(Plan* plan) {
    TopologicalDispatcher topo_dispatcher(true);
    topo_dispatcher.AddRule(new RemoveUnsinkedPassRule());
    return topo_dispatcher.Run(plan);
}

}  // namespace planner
}  // namespace flume
}  // namespace baidu
