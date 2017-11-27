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
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#ifndef FLUME_RUNTIME_LOCAL_LOCAL_EXECUTOR_FACTORY_H_
#define FLUME_RUNTIME_LOCAL_LOCAL_EXECUTOR_FACTORY_H_

#include <map>
#include <string>
#include <vector>

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/backend.h"
#include "flume/runtime/dataset.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/executor_factory.h"
namespace baidu {
namespace flume {
namespace runtime {
namespace local {

class LocalExecutorFactory : public ExecutorFactory {
public:
    LocalExecutorFactory(Backend* backend);

    void Initialize(const PbLocalJob& message,
                    DatasetManager* dataset_manager);

    virtual Executor* CreateExecutor(
            const PbExecutor& message,
            uint32_t scope_level,
            const std::vector<Executor*>& childs);

private:
    Executor* create_batch_executor(uint32_t scope_level, const PbExecutor& message,
            boost::ptr_vector<Executor>* batch_executors);

    DatasetManager* m_dataset_manager;
    std::map<std::string, PbLocalInput> m_inputs;
    Backend* m_backend;
};

}  // namespace local
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_LOCAL_LOCAL_EXECUTOR_FACTORY_H_
