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
//         Zhang Yuncong <zhangyuncong@baidu.com>
//         Wang Cong <wangcong09@baidu.com>

#ifndef FLUME_PLANNER_SPARK_MERGE_TASK_PASS_H
#define FLUME_PLANNER_SPARK_MERGE_TASK_PASS_H

#include "flume/planner/common/data_flow_analysis.h"
#include "flume/planner/common/promote_partial_units_pass.h"
#include "flume/planner/common/remove_empty_unit_pass.h"
#include "flume/planner/common/tags.h"
#include "flume/planner/spark/task_stage_analysis.h"
#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"
#include "flume/planner/plan.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

struct MergeTargetTask : public Pointer<Unit> {};

class MergeTaskPass : public Pass {
    RECURSIVE();
    RELY_PASS(MergeConcurrencyOneTaskAnalysis);
    RELY_PASS(MergeDownstreamTaskAnalysis);
    RELY_PASS(MergeBrotherTaskAnalysis);

    APPLY_PASS(PromotePartialUnitsPass);

public:
    class ClearTagPass : public Pass {
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class MergeConcurrencyOneTaskAnalysis : public Pass {
        RELY_PASS(ClearTagPass);
        RELY_PASS(TaskStageAnalysis);
        APPLY_PASS(MergeTaskActionPass);

    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class MergeDownstreamTaskAnalysis : public Pass {
        RELY_PASS(ClearTagPass);
        RELY_PASS(MergeConcurrencyOneTaskAnalysis);
        APPLY_PASS(MergeTaskActionPass);

    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class MergeBrotherTaskAnalysis : public Pass {
        RELY_PASS(ClearTagPass);
        RELY_PASS(MergeDownstreamTaskAnalysis);
        APPLY_PASS(MergeTaskActionPass);

    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class MergeTaskActionPass : public Pass {
        APPLY_PASS(RemoveEmptyUnitPass);

    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

private:
    virtual bool Run(Plan* plan) {
        return false;
    }
};

}  // namespace spark
}  // namespace planner
}  // namespace flume
}  // namespace baidu

#endif // FLUME_PLANNER_SPARK_MERGE_TASK_PASS_H

