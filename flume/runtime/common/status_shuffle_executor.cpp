/***************************************************************************
 *
 * Copyright (c) 2014 Baidu, Inc. All Rights Reserved.
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

#include "flume/runtime/common/status_shuffle_executor.h"

#include <netinet/in.h>

#include <string>
#include <utility>
#include <vector>

#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"
#include "toft/system/memory/unaligned.h"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/core/partitioner.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/shuffle_impl.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

void StatusShuffleCore::SetStatusTable(StatusTable* status_table) {
    m_status_table = status_table;
}

void StatusShuffleCore::DecodeKey(const toft::StringPiece& buffer, toft::StringPiece* key) {
    *key = buffer;
}

void StatusShuffleCore::DecodeKey(const toft::StringPiece& buffer, uint32_t* sequence) {
    *sequence = core::DecodePartition(buffer);
}

namespace {

template<typename Key>
class StatusShuffleCoreImpl : public StatusShuffleCore {
public:
    StatusShuffleCoreImpl() : m_base(NULL) {}

    virtual void Setup(const PbExecutor& message, ExecutorBase* base) {
        PbShuffleExecutor shuffle_message = message.shuffle_executor();

        m_base = base;
        m_identity = shuffle_message.scope().id();
        m_impl.Setup(shuffle_message.scope(), shuffle_message.node(), base);
    }

    virtual void BeginGroup(const std::vector<toft::StringPiece>& keys) {
        m_keys = keys;
        m_impl.StartShuffle();
    }

    virtual void EndGroup() {
        StatusTable::ScopeVisitor* scope_visitor =
                m_status_table->GetScopeVisitor(m_identity, m_keys);
        StatusTable::Iterator* iterator = scope_visitor->ListEntries();
        while (iterator->HasNext()) {
            typename internal::ShuffleImpl<Key, true>::KeyRef key;
            DecodeKey(iterator->NextValue(), &key);
            m_impl.MoveTo(key);
        }
        scope_visitor->Release();

        m_impl.FinishShuffle();
    }

private:
    ExecutorBase* m_base;
    std::string m_identity;
    internal::ShuffleImpl<Key, true> m_impl;

    std::vector<toft::StringPiece> m_keys;
};

template class StatusShuffleCoreImpl<std::string>;
template class StatusShuffleCoreImpl<uint32_t>;

}  // namespace

StatusShuffleCore* CreateStatusShuffleCore(const PbExecutor& message) {
    CHECK_EQ(message.type(), PbExecutor::SHUFFLE);
    CHECK_EQ(message.shuffle_executor().type(), PbShuffleExecutor::LOCAL);

    switch (message.shuffle_executor().scope().type()) {
        case PbScope::GROUP:
            return new StatusShuffleCoreImpl<std::string>();
        case PbScope::BUCKET:
            return new StatusShuffleCoreImpl<uint32_t>();
        default:
            LOG(FATAL) << "unexpected type for StatusShuffleExecutor: " << message.DebugString();
    }

    return NULL;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
