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
// Author: Pan Yuchang(BDG)<bigflow-opensource@baidu.com>
//         Wen Xiang <bigflow-opensource@baidu.com>

#ifndef FLUME_PLANNER_COMMON_SCOPE_ANALYSIS_H_
#define FLUME_PLANNER_COMMON_SCOPE_ANALYSIS_H_

#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"

namespace baidu {
namespace flume {
namespace planner {

// this pass should only be applied before RuntimeOptimazing, where all control units are
// TASK or SCOPE.
class ScopeAnalysis : public Pass {
    ON_STAGE(LOGICAL_OPTIMIZING | TOPOLOGICAL_OPTIMIZING);
private:
    class Impl;

    virtual bool Run(Plan* plan);
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_PLANNER_COMMON_SCOPE_ANALYSIS_H_


