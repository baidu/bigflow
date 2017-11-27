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
// Author: Wang Song <bigflow-opensource@baidu.com>
//

#include "flume/planner/first_layer_dispatcher.h"
#include "flume/planner/plan.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

FirstLayerDispatcher::~FirstLayerDispatcher() {
}

bool FirstLayerDispatcher::Run(Plan* plan) {
    bool changed = false;
    Unit* root = plan->Root();
    Unit::Children children(root->begin(), root->end());
    for (Unit::iterator it = children.begin(); it != children.end(); ++it) {
        changed |= Dispatch(plan, *it);
    }
    return changed;
}

} // namespace planner
} // namespace flume
} // namespace baidu


