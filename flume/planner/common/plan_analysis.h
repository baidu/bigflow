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
// Author: Xu Yao <xuyao02@baidu.com>

#ifndef FLUME_PLANNER_COMMON_PLAN_ANALYSIS_H
#define FLUME_PLANNER_COMMON_PLAN_ANALYSIS_H

#include "flume/proto/entity.pb.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "toft/base/scoped_ptr.h"

namespace baidu {
namespace flume {
namespace planner {

class PlanAnalysis {
public:
    typedef std::map<int64_t, PbEntity> EntityMap;

public:
    PlanAnalysis();
    virtual ~PlanAnalysis();

    virtual void get_entities_from_physical_plan(
            const PbPhysicalPlan& physical_plan,
            EntityMap* entities);

    virtual void get_entities_from_logical_plan(
            const PbLogicalPlan& logical_plan,
            EntityMap* entities);

    virtual void set_entities_to_physical_plan(
            const EntityMap& entities,
            PbPhysicalPlan* physical_plan);

    virtual void set_entities_to_logical_plan(
            const EntityMap& entities,
            PbLogicalPlan* logical_plan);

private:
    class Impl;
    toft::scoped_ptr<Impl> _impl;
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_COMMON_SCOPE_ANALYSIS_H_


