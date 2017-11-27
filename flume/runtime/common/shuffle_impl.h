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

#ifndef FLUME_RUNTIME_COMMON_SHUFFLE_IMPL_H_
#define FLUME_RUNTIME_COMMON_SHUFFLE_IMPL_H_

#include <string>
#include <vector>

#include "boost/optional.hpp"
#include "toft/base/string/string_piece.h"

#include "flume/core/objector.h"
#include "flume/core/partitioner.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/executor_base.h"
#include "flume/runtime/common/shuffle_runner.h"
#include "flume/runtime/dispatcher.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace internal {

template<typename T> struct RefType;

template<>
struct RefType<std::string> {
    typedef toft::StringPiece type;
};

template<>
struct RefType<uint32_t> {
    typedef uint32_t type;
};

class ShuffleImplBase {
protected:
    static std::vector<Dataset*> GetDatasets(const std::vector<Dispatcher*>& outputs) {
        std::vector<Dataset*> datasets;
        for (size_t i = 0; i < outputs.size(); ++i) {
            datasets.push_back(outputs[i]->GetDataset(outputs[i]->GetScopeLevel()));
        }
        return datasets;
    }

    static void PushDatasets(const toft::StringPiece& key,
                             const std::vector<Dataset*>& datasets,
                             const std::vector<Dispatcher*>& outputs) {
        CHECK_EQ(datasets.size(), outputs.size());
        for (size_t i = 0; i < datasets.size(); ++i) {
            outputs[i]->BeginGroup(key, datasets[i]);
        }
    }

    static void ReleaseDatasets(const std::vector<Dataset*>& datasets) {
        for (size_t i = 0; i < datasets.size(); ++i) {
            datasets[i]->Release();
        }
    }

    toft::StringPiece SaveKey(uint32_t key, boost::optional<uint32_t>* ptr) {
        *ptr = key;

        m_key_buffer = core::EncodePartition(key);
        return m_key_buffer;
    }

    toft::StringPiece SaveKey(const toft::StringPiece key,
                              boost::optional<toft::StringPiece>* ptr) {
        const size_t kMaxBufferBytes = 32 * 1024;  // 32k
        if (m_key_buffer.capacity() > kMaxBufferBytes && m_key_buffer.capacity() > key.size()) {
            // shrink size
            key.as_string().swap(m_key_buffer);
        } else {
            key.copy_to_string(&m_key_buffer);
        }

        *ptr = m_key_buffer;
        return m_key_buffer;
    }

    void ClearKey() {
        std::string().swap(m_key_buffer);
    }

private:
    std::string m_key_buffer;
};

template<typename Key, bool kIsSorted = true>
class ShuffleImpl : public ShuffleImplBase {
public:
    typedef ::google::protobuf::RepeatedPtrField<PbLogicalPlanNode> NodeList;
    typedef typename RefType<Key>::type KeyRef;

    void Setup(const PbScope& scope, const NodeList& nodes, ExecutorBase* base);

    void StartShuffle();

    void FinishShuffle();

    bool IsShuffleDone();

    // return true if we has it, and save it ot *key;
    bool GetNextKey(KeyRef* key);

    void MoveToNext();

    // flush all datasets with preceding key, and push datasets with same key
    // return true if enter new group
    bool MoveTo(KeyRef key);

private:
    std::vector<Dispatcher*> m_broadcast_outputs;
    BroadcastRunner m_broadcast_runner;

    std::vector<Dispatcher*> m_cogroup_outputs;
    CoGroupRunner<Key, kIsSorted> m_cogroup_runner;

    ExecutorBase* m_base;
    // m_current_key is always less than key(m_ptr)
    boost::optional<KeyRef> m_current_key;
    typename CoGroupRunner<Key, kIsSorted>::Container::iterator m_ptr;
};

template<typename Key, bool kIsSorted>
void ShuffleImpl<Key, kIsSorted>::Setup(const PbScope& scope, const NodeList& nodes,
                                        ExecutorBase* base) {
    m_broadcast_runner.Initialize(scope);
    m_cogroup_runner.Initialize(scope);

    for (int i = 0; i < nodes.size(); ++i) {
        const PbLogicalPlanNode& node = nodes.Get(i);
        Source* input = base->GetInput(node.shuffle_node().from());
        Dispatcher* output = base->GetOutput(node.id());

        output->SetObjector(node.objector());
        if (node.shuffle_node().type() == PbShuffleNode::BROADCAST) {
            m_broadcast_outputs.push_back(output);
            m_broadcast_runner.AddShuffleInput(node, input);
        } else {
            m_cogroup_outputs.push_back(output);
            m_cogroup_runner.AddShuffleInput(node, input);
        }
    }

    m_base = base;
}

template<typename Key, bool kIsSorted>
void ShuffleImpl<Key, kIsSorted>::StartShuffle() {
    m_broadcast_runner.StartShuffle(GetDatasets(m_broadcast_outputs));
    m_cogroup_runner.StartShuffle(GetDatasets(m_cogroup_outputs));
    m_current_key = boost::none;
}

template<typename Key, bool kIsSorted>
void ShuffleImpl<Key, kIsSorted>::FinishShuffle() {
    CHECK(IsShuffleDone());

    if (m_current_key == boost::none) {
        m_ptr = m_cogroup_runner.begin();
    } else {
        m_base->EndSubGroup();
    }

    while (m_ptr != m_cogroup_runner.end()) {
        const std::string& key_name = m_cogroup_runner.key_name(m_ptr);

        PushDatasets(key_name, m_broadcast_runner.datasets(key_name), m_broadcast_outputs);
        PushDatasets(key_name, m_cogroup_runner.datasets(m_ptr), m_cogroup_outputs);
        m_base->BeginSubGroup(key_name);
        m_base->EndSubGroup();

        ++m_ptr;
    }

    m_broadcast_runner.FinishShuffle();
    m_cogroup_runner.FinishShuffle();
    ClearKey();
}

template<typename Key, bool kIsSorted>
bool ShuffleImpl<Key, kIsSorted>::IsShuffleDone() {
    return m_broadcast_runner.IsInputDone() && m_cogroup_runner.IsInputDone();
}

template<typename Key, bool kIsSorted>
bool ShuffleImpl<Key, kIsSorted>::GetNextKey(KeyRef* key) {
    if (m_ptr == m_cogroup_runner.end()) {
        return false;
    }

    *key = m_cogroup_runner.key(m_ptr);
    return true;
}

template<typename Key, bool kIsSorted>
void ShuffleImpl<Key, kIsSorted>::MoveToNext() {
    if (m_current_key != boost::none) {
        m_base->EndSubGroup();
    }

    CHECK(m_ptr != m_cogroup_runner.end());
    toft::StringPiece key = SaveKey(m_cogroup_runner.key(m_ptr), &m_current_key);

    PushDatasets(key, m_broadcast_runner.datasets(key), m_broadcast_outputs);
    PushDatasets(key, m_cogroup_runner.datasets(m_ptr), m_cogroup_outputs);
    m_base->BeginSubGroup(key);

    ++m_ptr;
}

template<typename Key, bool kIsSorted>
bool ShuffleImpl<Key, kIsSorted>::MoveTo(KeyRef key) {
    CHECK(IsShuffleDone());

    if (m_current_key != boost::none) {
        if (*m_current_key == key) {
            return false;  // same group
        }
    } else {
        m_ptr = m_cogroup_runner.begin();
    }

    KeyRef next_key;
    while (GetNextKey(&next_key) && next_key <= key) {
        MoveToNext();
    }

    if (m_current_key != boost::none && *m_current_key != key) {
        m_base->EndSubGroup();
        m_current_key = boost::none;
    }

    if (m_current_key == boost::none) {
        toft::StringPiece group_key = SaveKey(key, &m_current_key);
        PushDatasets(group_key, m_broadcast_runner.datasets(group_key), m_broadcast_outputs);
        m_base->BeginSubGroup(group_key);
    }

    return true;  // new group
}

}  // namespace internal
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_SHUFFLE_IMPL_H_
