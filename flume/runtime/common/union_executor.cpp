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

#include "flume/runtime/common/union_executor.h"

#include <string>
#include <vector>

#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

using core::Entity;
using core::Objector;

void UnionRunner::setup(const PbExecutor& message, ExecutorContext* context) {
    CHECK_EQ(message.output_size(), 1);
    Dispatcher* output = context->output(message.output(0));

    for (int i = 0; i < message.input_size(); ++i) {
        Source* input = context->input(message.input(i));
        _inputs.push_back(input->RequireStream(
                Source::REQUIRE_OBJECT,
                toft::NewPermanentClosure(this, &UnionRunner::on_input_come, output),
                NULL
        ));
    }
}

void UnionRunner::on_input_come(Dispatcher* output, const std::vector<toft::StringPiece>& keys,
                                void* object, const toft::StringPiece& binary) {
    if (!output->EmitObject(object)) {
        for (size_t i = 0; i < _inputs.size(); ++i) {
            _inputs[i]->Done();
        }
    }
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
