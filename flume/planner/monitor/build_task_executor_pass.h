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
// Author: Wen Xiang <wenxiang@baidu.com>
//

#ifndef FLUME_PLANNER_MONITOR_BUILD_TASK_EXECUTOR_PASS_H_
#define FLUME_PLANNER_MONITOR_BUILD_TASK_EXECUTOR_PASS_H_

#include "flume/planner/common/build_common_executor_pass.h"
#include "flume/planner/common/build_executor_message_pass.h"
#include "flume/planner/common/scope_level_analysis.h"
#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"

namespace baidu {
namespace flume {
namespace planner {
namespace monitor {

class BuildTaskExecutorPass : public Pass {
    RELY_PASS(BuildCommonExecutorPass);
    RELY_PASS(BuildExecutorMessagePass);
    RELY_PASS(ScopeLevelAnalysis);
    PRESERVE_BY_DEFAULT();
public:
    virtual ~BuildTaskExecutorPass();

    virtual bool Run(Plan* plan);
};

}  // namespace monitor
}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_MONITOR_BUILD_TASK_EXECUTOR_PASS_H_
