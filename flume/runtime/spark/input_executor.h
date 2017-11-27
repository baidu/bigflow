/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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

//
// Created by zhangyuncong on 2017/9/14.
//

#ifndef FLUME_RUNTIME_SPARK_INPUT_EXECUTOR_H
#define FLUME_RUNTIME_SPARK_INPUT_EXECUTOR_H

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/dataset.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

class InputExecutor : public Executor {
public:
    virtual ~InputExecutor() {}

    virtual void initialize(const PbExecutor& message,
                    const std::vector<Executor*>& childs,
                    DatasetManager* dataset_manager) = 0;

    virtual bool process_input(const toft::StringPiece& key, const toft::StringPiece& value) = 0;

    virtual void input_done() = 0;

protected:
    // Executor Interfaces
    virtual void Setup(const std::map<std::string, Source*>& sources) = 0;
    virtual Source* GetSource(const std::string& id, unsigned scope_level) = 0;
    virtual void BeginGroup(const toft::StringPiece& key) = 0;
    virtual void FinishGroup() = 0;

};

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_SPARK_INPUT_EXECUTOR_H
