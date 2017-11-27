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

#ifndef FLUME_RUNTIME_COMMON_SINKER_EXECUTOR_H_
#define FLUME_RUNTIME_COMMON_SINKER_EXECUTOR_H_

#include <map>
#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/sinker.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

class SinkerExecutor : public Executor {
public:
    void Initialize(const PbExecutor& message);

    virtual void Setup(const std::map<std::string, Source*>& sources);
    virtual Source* GetSource(const std::string& id, unsigned scope_level);
    virtual void BeginGroup(const toft::StringPiece& key);
    virtual void FinishGroup();

private:
    void OnInputCome(const std::vector<toft::StringPiece>& keys,
                     void* object, const toft::StringPiece& binary);

    PbExecutor m_message;
    toft::scoped_ptr<core::Sinker> m_sinker;

    std::vector<toft::StringPiece> m_keys;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_SINKER_EXECUTOR_H_
