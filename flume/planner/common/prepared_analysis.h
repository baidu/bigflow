/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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
// Author: Wang Song <wangsong06@baidu.com>
//         Wen Xiang <wenxiang@baidu.com>

#ifndef FLUME_PLANNER_COMMON_PREPARED_ANALYSIS_H_
#define FLUME_PLANNER_COMMON_PREPARED_ANALYSIS_H_

#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/common/executor_dependency_analysis.h"
#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"

namespace baidu {
namespace flume {
namespace planner {

class PreparedAnalysis : public Pass {
    PRESERVE_BY_DEFAULT();
    RELY_PASS(DataFlowAnalysis);
    RELY_PASS(ExecutorDependencyAnalysis);

private:
    class Impl;

    virtual bool Run(Plan* plan);
};

}   // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_COMMON_PREPARED_ANALYSIS_H_
