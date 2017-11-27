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

#include "flume/runtime/local/local_input_executor.h"

#include <algorithm>
#include <iterator>
#include <string>
#include <vector>

#include "toft/base/closure.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

void LocalInputExecutor::Initialize(const PbLocalInput& local_input, const PbExecutor& executor,
                                    const std::vector<Executor*>& childs) {
    m_spliter = core::Entity<core::Loader>::From(local_input.spliter());
    std::copy(local_input.uri().begin(), local_input.uri().end(), std::back_inserter(m_uri_list));

    m_sub_executors.Initialize(executor, childs);
}

void LocalInputExecutor::Setup(const std::map<std::string, Source*>& sources) {
    m_sub_executors.Setup(sources);
}

Source* LocalInputExecutor::GetSource(const std::string& id, unsigned scope_level) {
    return m_sub_executors.GetSource(id, scope_level);
}

void LocalInputExecutor::BeginGroup(const toft::StringPiece& key) {
    m_sub_executors.BeginGroup(key);
}

void LocalInputExecutor::FinishGroup() {
    toft::scoped_ptr<core::Loader> spliter(m_spliter.CreateAndSetup());
    for (size_t i = 0; i < m_uri_list.size(); ++i) {
        std::vector<std::string> splits;
        spliter->Split(m_uri_list[i], &splits);

        for (size_t j = 0; j < splits.size(); ++j) {
            m_sub_executors.BeginGroup(splits[j]);
            m_sub_executors.FinishGroup();
        }
    }
    m_sub_executors.FinishGroup();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
