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
// Author: Pan Yuchang <panyuchang@baidu.com>
//         Wang Cong <wangcong09@baidu.com>
//

#ifndef FLUME_PLANNER_SPARK_BUILD_PHYSICAL_PLAN_PASS_H_
#define FLUME_PLANNER_SPARK_BUILD_PHYSICAL_PLAN_PASS_H_

#include "flume/planner/spark/build_task_executor_pass.h"
#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"
#include "flume/planner/plan.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class BuildPhysicalPlanPass : public Pass {
    PRESERVE_BY_DEFAULT();
    RELY_PASS(BuildTaskExecutorPass);
public:
    virtual ~BuildPhysicalPlanPass();

    virtual bool Run(Plan* plan);
};

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_SPARK_BUILD_PHYSICAL_PLAN_PASS_H_
