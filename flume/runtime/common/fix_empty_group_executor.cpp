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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>

#include "flume/runtime/common/fix_empty_group_executor.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/storage/file/file.h"
#include "toft/storage/seqfile/local_sequence_file_writer.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/dispatcher.h"
#include "flume/util/path_util.h"

namespace baidu {
namespace flume {
namespace runtime {

void CreateEmptyRecordExecutor::setup(const PbExecutor& executor, ExecutorContext* context) {
    CHECK_EQ(1u, executor.input_size());
    std::string id = executor.create_empty_record_executor().output();
    _output = context->output(id);
    Source* source = context->input(executor.input(0));
    _handle = source->RequireStream(
            Source::REQUIRE_BINARY,
            toft::NewPermanentClosure(this, &CreateEmptyRecordExecutor::on_input_come),
            NULL);
}

void CreateEmptyRecordExecutor::begin_group(const std::vector<toft::StringPiece>& keys) {
    _is_empty = true;
}

void CreateEmptyRecordExecutor::on_input_come(
        const std::vector<toft::StringPiece>& keys,
        void* object,
        const toft::StringPiece& binary) {
    _is_empty = false;
    if (!_output->EmitBinary(binary)) {
        _handle->Done();
    }
}

void CreateEmptyRecordExecutor::end_group() {

    if (_is_empty) {
        _output->EmitBinary(toft::StringPiece());
        // binary whose data() == NULL means empty group
    }
}

void FilterEmptyRecordExecutor::setup(const PbExecutor& executor, ExecutorContext* context) {
    CHECK_EQ(1u, executor.input_size());
    std::string id = executor.filter_empty_record_executor().output();
    Dispatcher* output = context->output(id);
    Source* source = context->input(executor.input(0));
    Source::Handle* handle = source->RequireStream(
            Source::REQUIRE_BINARY,
            toft::NewPermanentClosure(
                    this, &FilterEmptyRecordExecutor::on_input_come, handle, output),
            NULL);
}

void FilterEmptyRecordExecutor::begin_group(const std::vector<toft::StringPiece>& keys) {
}

void FilterEmptyRecordExecutor::on_input_come(
        Source::Handle* handle,
        Dispatcher* dispatcher,
        const std::vector<toft::StringPiece>& keys,
        void* object,
        const toft::StringPiece& binary
        ) {
    if (binary.data() != NULL) {
        if (!dispatcher->EmitBinary(binary)) {
            handle->Done();
        }
    }
}

void FilterEmptyRecordExecutor::end_group() {
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
