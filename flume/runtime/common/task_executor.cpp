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

#include "flume/runtime/common/task_executor.h"

#include <string>
#include <vector>

#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/processor.h"
#include "flume/planner/graph_helper.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

void TaskExecutor::Initialize(const PbExecutor& message,
                                 const std::vector<Executor*>& childs) {
    m_childs.Initialize(message, childs);
}

void TaskExecutor::Setup(const std::map<std::string, Source*>& sources) {
    m_childs.Setup(sources);
}

Source* TaskExecutor::GetSource(const std::string&id, unsigned scope_level) {
    return m_childs.GetSource(id, scope_level);
}

void TaskExecutor::BeginGroup(const toft::StringPiece& key) {
    m_childs.BeginGroup(key);
}

void TaskExecutor::FinishGroup() {
    m_childs.FinishGroup();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
