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

#ifndef FLUME_RUNTIME_LOCAL_LOCAL_INPUT_EXECUTOR_H_
#define FLUME_RUNTIME_LOCAL_LOCAL_INPUT_EXECUTOR_H_

#include <map>
#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/loader.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/sub_executor_manager.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

class LocalInputExecutor : public Executor {
public:
    virtual ~LocalInputExecutor() {}

    void Initialize(const PbLocalInput& local_input, const PbExecutor& executor,
                    const std::vector<Executor*>& childs);

    virtual void Setup(const std::map<std::string, Source*>& sources);
    virtual Source* GetSource(const std::string& id, unsigned scope_level);
    virtual void BeginGroup(const toft::StringPiece& key);
    virtual void FinishGroup();

private:
    core::Entity<core::Loader> m_spliter;
    std::vector<std::string> m_uri_list;
    SubExecutorManager m_sub_executors;
};


}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_LOCAL_LOCAL_INPUT_EXECUTOR_H_
