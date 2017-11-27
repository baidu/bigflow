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
// Author: Wen Xiang <wenxiang@baidu.com>
//
// Interface.

#ifndef FLUME_RUNTIME_EXECUTOR_FACTORY_H_
#define FLUME_RUNTIME_EXECUTOR_FACTORY_H_

#include <vector>

#include "flume/runtime/dataset.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/backend.h"

namespace baidu {
namespace flume {

class PbExecutor;  // flume/proto/physical_plan.proto

namespace runtime {

class ExecutorFactory {
public:
    ExecutorFactory() : m_backend(NULL) { }
    virtual ~ExecutorFactory() {}

    void SetBackend(Backend *backend) { m_backend = backend; }

    virtual Executor* CreateExecutor(
            const PbExecutor& message,
            uint32_t scope_level,
            const std::vector<Executor*>& childs) = 0;

public:
    static Executor* CreateCommonExecutor(
            const PbExecutor& message,
            uint32_t scope_level,
            const std::vector<Executor*>& childs,
            DatasetManager* dataset_manager);
private:
    Backend *m_backend;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_EXECUTOR_FACTORY_H_
