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
// Author: Pan Yuchang<bigflow-opensource@baidu.com>
/*
 * This pass is used to handle the situation when more than one
 * dependency appear between two nodes.
 * i.e.        ----                        ----
 *            /    \                      /    \
 *       load       union   or    process1      process2
 *            \    /                      \    /
 *             ----                        ----
 *
 * after this pass, the topo will be like this
 *
 *             -- union --                        -- union --
 *            /           \                      /           \
 *       load              union   or    process1             process2
 *            \           /                      \           /
 *             -----------                        -----------
 *
 * and the -- union -- node is added as a proxy of first node (which in pic is load, process1)
 *
 * */


#ifndef FLUME_PLANNER_COMMON_SPLIT_UNION_UNIT_PASS_H
#define FLUME_PLANNER_COMMON_SPLIT_UNION_UNIT_PASS_H

#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"

namespace baidu {
namespace flume {
namespace planner {

class SplitUnionUnitPass : public Pass {
public:
    virtual ~SplitUnionUnitPass();

    virtual bool Run(Plan* plan);
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_COMMON_SPLIT_UNION_UNIT_PASS_H_
