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

#ifndef FLUME_PLANNER_COMMON_REMOVE_UNSINKED_PASS_H_
#define FLUME_PLANNER_COMMON_REMOVE_UNSINKED_PASS_H_

#include <set>
#include <string>

#include "boost/optional.hpp"
#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"
#include "flume/planner/common/side_effect_analysis.h"
#include "flume/planner/common/group_generator_analysis.h"
#include "flume/planner/plan.h"

namespace baidu {
namespace flume {
namespace planner {

class RemoveUnsinkedPass : public Pass {
    RELY_PASS(SideEffectAnalysis);
    RELY_PASS(GroupGeneratorAnalysis);
    RECURSIVE();
public:
    virtual ~RemoveUnsinkedPass();

    virtual bool Run(Plan* plan);
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_COMMON_REMOVE_UNSINKED_PASS_H_
