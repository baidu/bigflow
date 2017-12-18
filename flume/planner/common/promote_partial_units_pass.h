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

#ifndef FLUME_PLANNER_COMMON_PROMOTE_PARTIAL_UNITS_PASS_H_
#define FLUME_PLANNER_COMMON_PROMOTE_PARTIAL_UNITS_PASS_H_

#include "flume/planner/common/remove_unsinked_pass.h"
#include "flume/planner/common/remove_useless_union_pass.h"
#include "flume/planner/common/util.h"
#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"
#include "flume/planner/unit.h"

namespace baidu {
namespace flume {
namespace planner {

class PromotePartialUnitsPass : public Pass {
public:
    RELY_PASS(PromotableAnalysis);
    RELY_PASS(PromoteUnitsPass);
    RELY_PASS(RemoveUselessUnionPass);
    RELY_PASS(RemoveUnsinkedPass);

    class FullShuffleCountAnalysis : public Pass {
    public:
        PRESERVE_BY_DEFAULT();

    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class PromotableAnalysis : public Pass {
    public:
        RELY_PASS(FullShuffleCountAnalysis);
        PRESERVE_BY_DEFAULT();
        INVALIDATE_PASS(PromoteUnitsPass);

    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class PromoteUnitsPass : public Pass {
    public:
        RECURSIVE();
        HOLD_BY_DEFAULT();

    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    struct FullShuffleCount : public Value<int> {};
    struct ShouldPromote {};
    struct IsPromoted {};
    struct OnlyClone {};
    struct Cloned {};
    struct DistributeEveryScopeUnit : Pointer<Unit> {};
    struct UnitInputTasks : std::set<Unit*> {};
    struct IsUnderDistributeEveryScope {};

private:
    class Impl;

    virtual bool Run(Plan* plan);
};

}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_COMMON_PROMOTE_PARTIAL_UNITS_PASS_H_
