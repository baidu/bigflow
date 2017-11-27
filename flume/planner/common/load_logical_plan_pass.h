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
// Author: Wen Xiang <wenxiang@baidu.com>

#ifndef FLUME_PLANNER_COMMON_LOAD_LOGICAL_PLAN_PASS_H_
#define FLUME_PLANNER_COMMON_LOAD_LOGICAL_PLAN_PASS_H_

#include <set>
#include <utility>
#include <vector>

#include "flume/planner/common/cache_analysis.h"
#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"
#include "flume/proto/logical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {

class Unit;
class Plan;

// Add units and calls set<PbScope>/set<PbLogicalPlanNode> according to logical_plan
class LoadLogicalPlanPass : public Pass {
    HOLD_BY_DEFAULT();
    INVALIDATE_PASS(CacheAnalysis);

public:
    void Initialize(const PbLogicalPlan& logical_plan);

    virtual bool Run(Plan* plan);

private:
    PbLogicalPlan m_logical_plan;
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_COMMON_LOAD_LOGICAL_PLAN_PASS_H_
