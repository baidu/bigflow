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
//
// A helper class, contains logics related to sub-executor management.

#ifndef FLUME_RUNTIME_COMMON_SUB_EXECUTOR_MANAGER_H_
#define FLUME_RUNTIME_COMMON_SUB_EXECUTOR_MANAGER_H_

#include <map>
#include <string>
#include <vector>

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

class Source;

// SubExecutorManager is used to manage child executors. See flume/doc/executor.rst
// for details about executor hierarchy.
// Three works are done by SubExecutorManager:
//      1. Setup data flow relationships of father/childs and child/child.
//      2. Call BeginGroup method for each child in topological order according to data
//         flow.
//      3. Call FinishGroup method for each child in reverse topological order.
// SubExecutorManager is to be used by father executor, see MemoryShuffleExecutor for
// example (flume/runtime/common/memory_shuffle_executor.h).
class SubExecutorManager {
public:
    // message: proto message for father executor
    // childs: instances of all childs. In the same order of message.child()
    // returns: sources which are needed by childs
    void Initialize(const PbExecutor& message, const std::vector<Executor*>& childs);

    // Setup data flow relationships.
    // inputs: sources that should provided by father executor.
    void Setup(const std::map<std::string, Source*>& inputs);

    // Called after Setup. Get source which is provided by one of childs.
    Source* GetSource(const std::string& identity, int scope_level);

    // Call BeginGroup in each child.
    void BeginGroup(const toft::StringPiece& key);

    // Call FinishGroup in each child.
    void FinishGroup();

private:
    PbExecutor m_message;
    std::vector<Executor*> m_childs;
    std::map<std::string, Executor*> m_outputs;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_SUB_EXECUTOR_MANAGER_H_
