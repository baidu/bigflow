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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>
//         Wang Cong <wangcong09@baidu.com>
//

#ifndef FLUME_PLANNER_SPARK_ADD_TRANSFER_EXECUTOR_PASS_H_
#define FLUME_PLANNER_SPARK_ADD_TRANSFER_EXECUTOR_PASS_H_

#include "flume/planner/pass.h"
#include "flume/planner/pass_manager.h"
#include "flume/planner/common/add_common_executor_pass.h"

namespace baidu {
namespace flume {
namespace planner {
namespace spark {

class AddTransferExecutorPass : public Pass {
public:
    RELY_PASS(AddHadoopInputPass);
    RELY_PASS(AddShuffleInputPass);
    RELY_PASS(AddShuffleOutputPass);

    class AddHadoopInputPass : public Pass {
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class AddShuffleInputPass : public Pass {
    private:
        class Impl;

        virtual bool Run(Plan* plan);
    };

    class AddShuffleOutputPass : public Pass {
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

#endif // FLUME_PLANNER_SPARK_ADD_TRANSFER_EXECUTOR_PASS_H_
