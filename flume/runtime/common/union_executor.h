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

#ifndef FLUME_RUNTIME_COMMON_UNION_EXECUTOR_H_
#define FLUME_RUNTIME_COMMON_UNION_EXECUTOR_H_

#include <map>
#include <string>
#include <vector>

#include "boost/dynamic_bitset.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/scoped_ptr.hpp"
#include "toft/base/string/string_piece.h"

#include "flume/core/objector.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/executor_impl.h"
#include "flume/runtime/common/general_dispatcher.h"
#include "flume/runtime/executor.h"
#include "flume/util/bitset.h"

namespace baidu {
namespace flume {
namespace runtime {

class UnionRunner : public ExecutorRunner {
public:
    virtual ~UnionRunner() {}

    virtual void setup(const PbExecutor& message, ExecutorContext* context);

    // called when all inputs is ready
    virtual void begin_group(const std::vector<toft::StringPiece>& keys) {}

    // called after all inputs is done.
    virtual void end_group() {}

private:
    void on_input_come(Dispatcher* output, const std::vector<toft::StringPiece>& keys,
                       void* object, const toft::StringPiece& binary);

    std::vector<Source::Handle*> _inputs;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_UNION_EXECUTOR_H_
