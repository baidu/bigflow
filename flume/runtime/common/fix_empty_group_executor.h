/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>

#ifndef BLADE_FLUME_RUNTIME_COMMON_CREATE_EMPTY_RECORD_EXECUTOR_H
#define BLADE_FLUME_RUNTIME_COMMON_CREATE_EMPTY_RECORD_EXECUTOR_H

#include "toft/base/scoped_ptr.h"

#include "flume/runtime/common/executor_base.h"
#include "flume/runtime/dispatcher.h"

namespace baidu {
namespace flume {
namespace runtime {

class Dispatcher;

class CreateEmptyRecordExecutor : public ExecutorRunner {
public:
    CreateEmptyRecordExecutor() {}

    virtual void setup(const PbExecutor& executor, ExecutorContext* context);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys);

    virtual void end_group();

    void on_input_come(
        const std::vector<toft::StringPiece>& keys,
        void* object,
        const toft::StringPiece& binary);

private:
    Dispatcher* _output;
    std::vector<toft::StringPiece> _keys;
    bool _is_empty;
    Source::Handle* _handle;
};

class FilterEmptyRecordExecutor : public ExecutorRunner {
public:
    FilterEmptyRecordExecutor() {}

    virtual void setup(const PbExecutor& executor, ExecutorContext* context);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys);

    virtual void end_group();

    void on_input_come(
        Source::Handle* handle,
        Dispatcher* output,
        const std::vector<toft::StringPiece>& keys,
        void* object,
        const toft::StringPiece& binary
        );
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // BLADE_FLUME_RUNTIME_COMMON_CREATE_EMPTY_RECORD_EXECUTOR_H
