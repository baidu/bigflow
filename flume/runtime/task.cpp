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

#include "flume/runtime/task.h"

#include <map>
#include <string>
#include <vector>

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/executor_factory.h"

namespace baidu {
namespace flume {
namespace runtime {

void Task::Initialize(const PbExecutor& message, ExecutorFactory* factory) {
    m_message = message;

    //reset executors
    m_root = NULL;
    m_executors.clear();

    m_root = CreateExecutor(m_message, 0, factory);
}

bool Task::ChildIsInBatchMode(const PbExecutor& message) {
    if (message.type() != PbExecutor::STREAM_SHUFFLE) {
        return false;
    }
    const PbStreamShuffleExecutor& sub_message = message.stream_shuffle_executor();
    return sub_message.type() == PbStreamShuffleExecutor::DISTRIBUTE_AS_BATCH ||
        sub_message.type() == PbStreamShuffleExecutor::WINDOW;
}

Executor* Task::CreateExecutor(
        const PbExecutor& self,
        uint32_t scope_level,
        ExecutorFactory* factory) {
    std::vector<Executor*> childs;
    for (int i = 0; i < self.child_size(); ++i) {
        childs.push_back(CreateExecutor(self.child(i), self.scope_level(), factory));
    }

    Executor* executor = factory->CreateExecutor(self, scope_level, childs);
    m_executors.push_back(executor);
    return executor;
}

void Task::Run(const toft::StringPiece& info) {
    RunAsBatch(info);
}

void Task::RunAsBatch(const toft::StringPiece& info) {
    std::map<std::string, Source*> sources;
    m_root->Setup(sources);

    m_root->BeginGroup(info);
    m_root->FinishGroup();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
