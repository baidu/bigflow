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

#ifndef FLUME_PLANNER_COMMON_ADD_COMMON_EXECUTOR_PASS_H_
#define FLUME_PLANNER_COMMON_ADD_COMMON_EXECUTOR_PASS_H_

#include <map>
#include <string>

#include "flume/planner/common/remove_empty_unit_pass.h"
#include "flume/planner/common/remove_unsinked_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"

namespace baidu {
namespace flume {
namespace planner {

class Plan;
class Unit;
class AddCommonExecutorPassTest;

// a pass group, can be relied as a whole, or be inherited to used a subset of its
// sub-passes
class AddCommonExecutorPass : public Pass {
    RELY_PASS(AddPartialExecutorPass);
    RELY_PASS(AddShuffleExecutorPass);
    RELY_PASS(AddLogicalExecutorPass);
    RELY_PASS(AddCacheWriterExecutorPass);

public:
    class PartialAnalysis : public Pass {
        PRESERVE_BY_DEFAULT();
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class AddPartialExecutorPass : public Pass {
        RECURSIVE();
        RELY_PASS(PartialAnalysis);
        RELY_PASS(FixChannelUnitPass);
        RELY_PASS(RemoveUnsinkedPass);
        RELY_PASS(RemoveEmptyUnitPass);

        RELY_PASS(AddCacheWriterExecutorPass);
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class AddShuffleExecutorPass : public Pass {
        RELY_PASS(AddPartialExecutorPass);
        RELY_PASS(FixChannelUnitPass);
        RECURSIVE();
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class AddLogicalExecutorPass : public Pass {
        RELY_PASS(AddCacheWriterExecutorPass);
        RELY_PASS(AddPartialExecutorPass);
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    // In TopologicalOptimizing, a channel may have both in-task and cross-task froms.
    // This pass will make every channel only contains cross-task froms.
    class FixChannelUnitPass : public Pass {
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class AddCacheWriterExecutorPass : public Pass {
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    friend class AddCommonExecutorPassTest;
private:
    // tag if the functor of unit is KeyReader, Partitioner or Processor which do not
    // require key and prepared inputs.
    // provided by PartialAnalysis
    struct IsStreamingUnit {};

    // tag if a unit is already under PartialExecutor, directly or indirectly
    // provided by PartialAnalysis
    struct IsUnderPartialExecutor {};

    // tag if a unit should be moved under PartialExecutor. If an unit has such tag, it
    // will be captured by AddPartialExecutorPass and moved under PartialExecutor
    // provided by PartialAnalysis
    struct ShouldUnderPartialExecutor {};

    // tag on units under PartialExecutor, for leafs under ShuffleExecutor, which can be
    // executed separatedly from its brothers.
    struct ShouldBeSeparated : Pointer<Unit> {};  // point to the separation root
    struct IsSeparated {};

    virtual bool Run(Plan* plan) {
        return false;
    }
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_COMMON_ADD_COMMON_EXECUTOR_PASS_H_
