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

#include "flume/runtime/common/write_cache_executor.h"

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

namespace {

void OnInputCome(
        CacheManager::Writer* writer,
        const std::vector<toft::StringPiece>& keys,
        void* object,
        const toft::StringPiece& binary) {
    CHECK_NOTNULL(writer);
    writer->Write(binary);
}

void OnInputDone() {}

} // namespace

void WriteCacheExecutor::Setup(const PbExecutor& executor, ExecutorBase* base) {
    CHECK_NOTNULL(m_cache_manager);
    m_writer = m_cache_manager->GetWriter(executor.write_cache_executor().from());
    CHECK_EQ(1u, executor.input_size());
    Source* source = base->GetInput(executor.input(0));
    source->RequireStream(Source::REQUIRE_BINARY,
        toft::NewPermanentClosure(&OnInputCome, m_writer),
        toft::NewPermanentClosure(OnInputDone));
    m_effective_key_num = executor.write_cache_executor().key_num();

    if (m_effective_key_num != -1) {
        m_keys.resize(m_effective_key_num + 1); // the attamp_id and the effective_keys
    }
}

void WriteCacheExecutor::BeginGroup(const std::vector<toft::StringPiece>& keys) {
    if (m_effective_key_num != -1) {
        m_keys[0] = keys[0];
        for (int i = 0; i != m_effective_key_num; ++i) {
            m_keys[i + 1] = keys[keys.size() - m_effective_key_num + i];
        }
        m_writer->BeginKeys(m_keys);
    } else {
        m_writer->BeginKeys(keys);
    }
}

void WriteCacheExecutor::EndGroup() {
    m_writer->EndKeys();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
