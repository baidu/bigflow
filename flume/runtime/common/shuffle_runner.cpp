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

#include "flume/runtime/common/shuffle_runner.h"

#include <string>
#include <utility>
#include <vector>

#include "gflags/gflags.h"
#include "boost/bind.hpp"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/core/partitioner.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/dataset.h"
#include "flume/runtime/executor.h"
#include "flume/util/arena.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace internal {

using core::Entity;
using core::Objector;
using core::KeyReader;
using core::Partitioner;

static const size_t kKeyBufferSize = 32 * 1024;  // 32k

BroadcastRunner::BroadcastRunner() {}

void BroadcastRunner::Initialize(const PbScope& scope) {
    m_message = scope;
}

void BroadcastRunner::AddShuffleInput(const PbLogicalPlanNode& node, Source* source) {
    CHECK_EQ(PbLogicalPlanNode::SHUFFLE_NODE, node.type());
    CHECK_EQ(PbShuffleNode::BROADCAST, node.shuffle_node().type());

    ShuffleInfo info;
    info.node = node;
    info.input = source->RequireStream(Source::REQUIRE_BINARY,
            toft::NewPermanentClosure(this, &BroadcastRunner::OnInputCome, m_shuffles.size()),
            toft::NewPermanentClosure(this, &BroadcastRunner::OnInputDone, m_shuffles.size())
    );
    m_shuffles.push_back(info);

    m_active_inputs.push_back(false);
}

void BroadcastRunner::StartShuffle(const std::vector<Dataset*>& outputs) {
    CHECK_EQ(m_shuffles.size(), outputs.size());
    m_active_inputs.set();

    for (size_t i = 0; i < m_shuffles.size(); ++i) {
        ShuffleInfo& shuffle = m_shuffles[i];

        shuffle.output = outputs[i];
        shuffle.temp = outputs[i]->GetChild("");
    }
}

void BroadcastRunner::FinishShuffle() {
    for (size_t i = 0; i < m_shuffles.size(); ++i) {
        ShuffleInfo& shuffle = m_shuffles[i];

        shuffle.result->Done();
        shuffle.temp->Release();

        shuffle.output = NULL;
        shuffle.temp = NULL;
        shuffle.result = NULL;
    }
}

bool BroadcastRunner::IsInputDone() {
    return m_active_inputs.none();
}

void BroadcastRunner::OnInputCome(int index, const std::vector<toft::StringPiece>& keys,
                                  void* object, const toft::StringPiece& binary) {
    CopyTo(binary, m_shuffles[index].temp);
}

void BroadcastRunner::OnInputDone(int index) {
    m_active_inputs[index] = false;

    ShuffleInfo& shuffle = m_shuffles[index];
    shuffle.result = shuffle.temp->Discard();
}

std::vector<Dataset*> BroadcastRunner::datasets(const toft::StringPiece& key) {
    CHECK(IsInputDone());

    std::vector<Dataset*> results;
    for (size_t i = 0; i < m_shuffles.size(); ++i) {
        Dataset* dataset = m_shuffles[i].output->GetChild(key);

        Dataset::Iterator* iterator = m_shuffles[i].result;
        iterator->Reset();
        while (iterator->HasNext()) {
            CopyTo(iterator->NextValue(), dataset);
        }

        dataset->Commit();
        results.push_back(dataset);
    }

    return results;
}

KeyReaderFunctor::KeyReaderFunctor() : m_key_buffer(new char[kKeyBufferSize]) {}

void KeyReaderFunctor::Initialize(const PbScope& scope, const PbLogicalPlanNode& node) {
    CHECK_EQ(scope.id(), node.scope());
    CHECK_EQ(PbLogicalPlanNode::SHUFFLE_NODE, node.type());
    CHECK_EQ(PbShuffleNode::KEY, node.shuffle_node().type());

    Entity<KeyReader> entity = Entity<KeyReader>::From(node.shuffle_node().key_reader());
    m_key_reader.reset(entity.CreateAndSetup());
}

std::string KeyReaderFunctor::operator()(void* object) {
    toft::scoped_array<char> temporary_buffer;
    char* buffer = m_key_buffer.get();
    size_t buffer_size = kKeyBufferSize;

    size_t key_size = m_key_reader->ReadKey(object, buffer, buffer_size);
    if (key_size > buffer_size) {
        temporary_buffer.reset(new char[key_size]);
        buffer = temporary_buffer.get();
        buffer_size = key_size;

        key_size = m_key_reader->ReadKey(object, buffer, buffer_size);
        CHECK_LE(key_size, buffer_size);
    }
    return std::string(buffer, key_size);
}

toft::StringPiece KeyReaderFunctor::operator() (void* object, flume::util::Arena* arena) {
    return arena->Allocate(boost::bind(&KeyReader::ReadKey, m_key_reader.get(), object, _1, _2));
}

PartitionerFunctor::PartitionerFunctor() : m_partition_num(0), m_sequence(0) {}

void PartitionerFunctor::Initialize(const PbScope& scope, const PbLogicalPlanNode& node) {
    CHECK_EQ(scope.id(), node.scope());
    CHECK_EQ(PbLogicalPlanNode::SHUFFLE_NODE, node.type());
    CHECK_EQ(PbShuffleNode::SEQUENCE, node.shuffle_node().type());

    m_partition_num = scope.bucket_scope().bucket_size();
    if (node.shuffle_node().has_partitioner()) {
        Entity<Partitioner> entity = Entity<Partitioner>::From(node.shuffle_node().partitioner());
        m_partitioner.reset(entity.CreateAndSetup());
    }
}

uint32_t PartitionerFunctor::operator()(void* object) {
    if (m_partitioner == NULL) {
        return m_sequence++ % m_partition_num;
    }

    uint32_t key = m_partitioner->Partition(object, m_partition_num);
    DCHECK_LT(key, m_partition_num);
    ++m_sequence;
    return key;
}

}  // namespace internal
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
