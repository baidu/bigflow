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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>

#ifndef FLUME_PLANNER_GROUP_GENERATOR_ANALYSIS_H_
#define FLUME_PLANNER_GROUP_GENERATOR_ANALYSIS_H_

#include "flume/planner/common/cache_analysis.h"
#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"

namespace baidu {
namespace flume {
namespace planner {

// Analysis the shuffle node if it is a group generator
// A GroupGenerator is a shuffle node who may cause a new group
// eg.
// When there is a SN who is created by Load Cache, the original cache shuffle node
// will contain all the group needed, so, the original shuffle nodes is not group generator any more
class GroupGeneratorAnalysis : public Pass {
private:
    PRESERVE_BY_DEFAULT();
    virtual bool Run(Plan* plan);
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_GROUP_GENERATOR_ANALYSIS_H_
