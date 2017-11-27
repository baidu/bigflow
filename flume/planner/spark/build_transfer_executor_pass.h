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
// Author: Pan Yuchang <bigflow-opensource@baidu.com>
//         Zhang Yuncong <bigflow-opensource@baidu.com>
//         Wang Cong <bigflow-opensource@baidu.com>

#ifndef FLUME_PLANNER_SPARK_BUILD_TRANSFER_EXECUTOR_PASS_H_
#define FLUME_PLANNER_SPARK_BUILD_TRANSFER_EXECUTOR_PASS_H_

#include <vector>

#include "flume/planner/common/build_common_executor_pass.h"
#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"
#include "flume/planner/spark/init_rdd_info_pass.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class BuildTransferExecutorPass : public Pass {
public:
    RELY_PASS(BuildHadoopInputPass);
    RELY_PASS(BuildShuffleInputPass);
    RELY_PASS(BuildShuffleOutputPass);

    class BuildHadoopInputPass : public Pass {
        PRESERVE_BY_DEFAULT();
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class BuildShuffleInputPass : public Pass {
        PRESERVE_BY_DEFAULT();
        RELY_PASS(TransferAnalysis);
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class BuildShuffleOutputPass : public Pass {
        PRESERVE_BY_DEFAULT();
        RELY_PASS(TransferAnalysis);
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class TransferAnalysis : public Pass {
        PRESERVE_BY_DEFAULT();
        RELY_PASS(PreparedAnalysis);
        RELY_PASS(InitRddInfoPass);
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

#endif // FLUME_PLANNER_SPARK_BUILD_TRANSFER_EXECUTOR_PASS_H_
