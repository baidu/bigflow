/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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
// Author: Wang Cong <bigflow-opensource@baidu.com>

#include "flume/runtime/spark/cache_input_executor.h"

#include "flume/core/entity.h"
#include "flume/runtime/io/io_format.h"
#include "flume/planner/common/cache_util.h"
#include "flume/runtime/spark/shuffle_protocol.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

using baidu::flume::planner::CacheRecord;
using baidu::flume::planner::CacheRecordObjectorEntity;

CacheInputExecutor::CacheInputExecutor(const PbSparkTask::PbCacheInput& message): _message(message) {}

void CacheInputExecutor::initialize(
        const PbExecutor& message,
        const std::vector<Executor*>& childs,
        DatasetManager* dataset_manager) {

    _output.reset(new GeneralDispatcher(_message.id(), 1));
    _output->SetDatasetManager(dataset_manager);
    _output->SetObjector(CacheRecordObjectorEntity());
}

void parse_key(const toft::StringPiece& key, uint32_t key_num, std::vector<std::string>* keys) {
    keys->resize(key_num);

    if (key_num == 0) {
        return;
    }

    CHECK_GE(key.size(), sizeof(ShuffleHeader));

    const char* start = key.data() + sizeof(ShuffleHeader);
    size_t left = key.size() - sizeof(ShuffleHeader);
    uint32_t len;
    for (size_t i = 0; i < key_num - 1; ++i) {
        uint32_t ne_len; // network endian order length
        CHECK_GE(left, sizeof(ne_len));
        memcpy(&ne_len, start, sizeof(ne_len));
        len = ntohl(ne_len);
        CHECK_GE(left, sizeof(len) + len);
        (*keys)[i].assign(start + sizeof(len), len);
        start += len + sizeof(len);
        left -= len + sizeof(len);
    }
    keys->back().assign(start, left);
}

bool CacheInputExecutor::process_input(
        const toft::StringPiece& key,
        const toft::StringPiece& value) {

    CacheRecord record;
    // add 1 as key_num doesn't include global key
    parse_key(key, _message.key_num() + 1, &record.keys);
    record.content = value;
    record.empty = false;
    return _output->EmitObject(&record);
}

void CacheInputExecutor::input_done() {
    // done scope-1
    _output->Done();

    // finish scope-0
    _output->FinishGroup();
}

void CacheInputExecutor::Setup(const std::map<std::string, Source*>& sources) {
}

Source* CacheInputExecutor::GetSource(const std::string& id, unsigned scope_level) {
    return _output->GetSource(scope_level);
}

void CacheInputExecutor::BeginGroup(const toft::StringPiece& key) {
    _output->BeginGroup(key);
}

void CacheInputExecutor::FinishGroup() {
    // begin scope-1
    _output->BeginGroup("");
    _output->FinishGroup();
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
