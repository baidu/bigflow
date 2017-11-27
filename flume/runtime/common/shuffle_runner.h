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

#ifndef FLUME_RUNTIME_COMMON_SHUFFLE_RUNNER_H_
#define FLUME_RUNTIME_COMMON_SHUFFLE_RUNNER_H_

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "boost/shared_ptr.hpp"
#include "boost/unordered_map.hpp"
#include "gflags/gflags.h"
#include "toft/base/scoped_array.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/key_reader.h"
#include "flume/core/partitioner.h"
#include "flume/proto/logical_plan.pb.h"
#include "flume/runtime/dataset.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/executor.h"
#include "flume/util/arena.h"
#include "flume/util/bitset.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace internal {

template<typename Key>
class ShuffleFunctor;

template<typename Key, bool kIsSorted, typename Value>
class ShuffleContainer;

class ShuffleRunnerBase {
public:
    static const std::string& GetKeyName(const std::string& key) {
        return key;
    }

    static std::string GetKeyName(uint32_t key) {
        return core::EncodePartition(key);
    }

    static void CopyTo(const toft::StringPiece& str, Dataset* dataset) {
        char* buffer = dataset->AcquireArena()->AllocateBytes(str.size());
        memcpy(buffer, str.data(), str.size());

        dataset->Emit(toft::StringPiece(buffer, str.size()));
        dataset->ReleaseArena();
    }

    toft::StringPiece GetKey(const std::string& key) {
        return key;
    }
};

class BroadcastRunner : public ShuffleRunnerBase {
public:
    BroadcastRunner();

    void Initialize(const PbScope& scope);
    void AddShuffleInput(const PbLogicalPlanNode& node, Source* source);

    void StartShuffle(const std::vector<Dataset*>& outputs);
    void FinishShuffle();
    bool IsInputDone();

    std::vector<Dataset*> datasets(const toft::StringPiece& key);

private:
    typedef util::Bitset Bitset;

    struct ShuffleInfo {
        PbLogicalPlanNode node;
        Source::Handle* input;

        // TODO(wenxiang): we should disable accessing broadcasted ShuffleNode from outside scope.
        Dataset* output;  // create childs from this dataset
        Dataset* temp;  // will never be committed
        Dataset::Iterator* result;  // = temp->Discard()

        ShuffleInfo() : input(NULL), output(NULL), temp(NULL), result(NULL) {}
    };

    void OnInputCome(int index, const std::vector<toft::StringPiece>& keys,
                     void* object, const toft::StringPiece& binary);
    void OnInputDone(int index);

    PbScope m_message;
    std::vector<ShuffleInfo> m_shuffles;
    Bitset m_active_inputs;
};

template<typename Key, bool kIsSorted = true>
class CoGroupRunner : public ShuffleRunnerBase {
public:
    typedef std::vector<Dataset*> ResultType;
    typedef ShuffleContainer<Key, kIsSorted, ResultType> Container;
    typedef ShuffleFunctor<Key> Functor;

    CoGroupRunner();

    void Initialize(const PbScope& scope);
    void AddShuffleInput(const PbLogicalPlanNode& node, Source* source);

    void StartShuffle(const std::vector<Dataset*>& outputs);
    void FinishShuffle();
    bool IsInputDone();

    int size() const { return m_map.size(); }
    typename Container::iterator begin() { return m_map.begin(); }
    typename Container::iterator end() { return m_map.end(); }

    typename Container::KeyType key(typename Container::iterator ptr) { return m_map.key(ptr); }
    std::string key_name(typename Container::iterator ptr) { return GetKeyName(this->key(ptr)); }
    std::vector<Dataset*> datasets(typename Container::iterator ptr);

private:
    typedef boost::dynamic_bitset<> Bitset;

    struct ShuffleInfo {
        PbLogicalPlanNode node;
        boost::shared_ptr<Functor> functor;

        Source::Handle* input;
        Dataset* output;

        ShuffleInfo() : input(NULL), output(NULL) {}
    };

    void OnInputCome(int index, const std::vector<toft::StringPiece>& keys,
                     void* object, const toft::StringPiece& binary);
    void OnInputDone(int index);

    std::vector<Dataset*> NewDatasets(const toft::StringPiece& key);

    PbScope m_message;
    std::vector<ShuffleInfo> m_shuffles;
    Bitset m_active_inputs;
    Container m_map;
};

template<typename Key, bool kIsSorted>
CoGroupRunner<Key, kIsSorted>::CoGroupRunner() {}

template<typename Key, bool kIsSorted>
void CoGroupRunner<Key, kIsSorted>::Initialize(const PbScope& scope) {
    m_message = scope;
    m_map.Initialize(scope);
}

template<typename Key, bool kIsSorted>
void CoGroupRunner<Key, kIsSorted>::AddShuffleInput(const PbLogicalPlanNode& node,
                                                    Source* source) {
    boost::shared_ptr<Functor> functor(new Functor());
    CHECK_EQ(PbLogicalPlanNode::SHUFFLE_NODE, node.type());
    functor->Initialize(m_message, node);

    ShuffleInfo info;
    info.node = node;
    info.functor = functor;
    info.input = source->RequireStream(Source::REQUIRE_OBJECT | Source::REQUIRE_BINARY,
            toft::NewPermanentClosure(this, &CoGroupRunner::OnInputCome, m_shuffles.size()),
            toft::NewPermanentClosure(this, &CoGroupRunner::OnInputDone, m_shuffles.size())
    );
    m_shuffles.push_back(info);

    m_active_inputs.push_back(false);
}

template<typename Key, bool kIsSorted>
void CoGroupRunner<Key, kIsSorted>::StartShuffle(const std::vector<Dataset*>& outputs) {
    CHECK_EQ(m_shuffles.size(), outputs.size());
    m_active_inputs.set();

    for (size_t i = 0; i < m_shuffles.size(); ++i) {
        m_shuffles[i].output = outputs[i];
    }

    for (typename Container::iterator ptr = m_map.begin(); ptr != m_map.end(); ++ptr) {
        m_map.value(ptr) = NewDatasets(GetKeyName(m_map.key(ptr)));
    }
}

template<typename Key, bool kIsSorted>
void CoGroupRunner<Key, kIsSorted>::FinishShuffle() {
    for (size_t i = 0; i < m_shuffles.size(); ++i) {
        m_shuffles[i].output = NULL;
    }
    m_map.Reset();
}

template<typename Key, bool kIsSorted>
void CoGroupRunner<Key, kIsSorted>::OnInputCome(int index,
                                                const std::vector<toft::StringPiece>& keys,
                                                void* object, const toft::StringPiece& binary) {
    const Key& key = (*m_shuffles[index].functor)(object);

    std::vector<Dataset*>& results = m_map[key];
    if (results.empty()) {
        results = NewDatasets(GetKeyName(key));
    }
    CopyTo(binary, results[index]);
}

template<typename Key, bool kIsSorted>
void CoGroupRunner<Key, kIsSorted>::OnInputDone(int index) {
    m_active_inputs[index] = false;
    if (!m_active_inputs.none()) {
        return;
    }

    for (typename Container::iterator ptr = m_map.begin(); ptr != m_map.end(); ++ptr) {
        std::vector<Dataset*>& results = m_map.value(ptr);
        for (size_t i = 0; i < results.size(); ++i) {
            results[i]->Commit();
        }
    }
}

template<typename Key, bool kIsSorted>
std::vector<Dataset*> CoGroupRunner<Key, kIsSorted>::NewDatasets(const toft::StringPiece& key) {
    std::vector<Dataset*> results;
    for (size_t i = 0; i < m_shuffles.size(); ++i) {
        results.push_back(m_shuffles[i].output->GetChild(key));
    }
    return results;
}

template<typename Key, bool kIsSorted>
std::vector<Dataset*> CoGroupRunner<Key, kIsSorted>::datasets(typename Container::iterator ptr) {
    return m_map.value(ptr);
}

template<typename Key, bool kIsSorted>
bool CoGroupRunner<Key, kIsSorted>::IsInputDone() {
    return m_active_inputs.none();
}

class KeyReaderFunctor {
public:
    KeyReaderFunctor();
    void Initialize(const PbScope& scope, const PbLogicalPlanNode& node);

    std::string operator()(void* object);
    toft::StringPiece operator() (void* object, flume::util::Arena* arena);

private:
    toft::scoped_ptr<core::KeyReader> m_key_reader;
    toft::scoped_array<char> m_key_buffer;
};
template<>
class ShuffleFunctor<std::string> : public KeyReaderFunctor {};

class PartitionerFunctor {
public:
    PartitionerFunctor();
    void Initialize(const PbScope& scope, const PbLogicalPlanNode& node);

    uint32_t operator()(void* object);

private:
    toft::scoped_ptr<core::Partitioner> m_partitioner;
    uint32_t m_partition_num;
    uint64_t m_sequence;
};
template<>
class ShuffleFunctor<uint32_t> : public PartitionerFunctor {};

template<typename Value>
class ShuffleContainer<
    std::string, true, Value
> : public std::map<std::string, Value> {
public:
    typedef const std::string& KeyType;
    typedef Value& ValueType;

public:
    void Initialize(const PbScope& scope) {
    }

    void Reset() {
        std::map<std::string, Value> tmp;
        this->swap(tmp);  // also release memory. a stl trick
    }

    KeyType key(typename ShuffleContainer::iterator ptr) {
        return ptr->first;
    }

    ValueType value(typename ShuffleContainer::iterator ptr) {
        return ptr->second;
    }
};

template<typename Value>
class ShuffleContainer<
    std::string, false, Value
> : public boost::unordered_map<std::string, Value> {
public:
    typedef const std::string& KeyType;
    typedef Value& ValueType;

public:
    void Initialize(const PbScope& scope) {
        CHECK_EQ(false, scope.is_sorted());
    }

    void Reset() {
        boost::unordered_map<std::string, Value> tmp;
        this->swap(tmp);  // also release memory. a stl trick
    }

    KeyType key(typename ShuffleContainer::iterator ptr) {
        return ptr->first;
    }

    ValueType value(typename ShuffleContainer::iterator ptr) {
        return ptr->second;
    }
};

template<bool kIsSorted, typename Value>
class ShuffleContainer<uint32_t, kIsSorted, Value> : public std::vector<Value> {
public:
    typedef uint32_t KeyType;
    typedef Value& ValueType;

public:
    ShuffleContainer() : m_bucket_number(0) {}

    void Initialize(const PbScope& scope) {
        m_bucket_number = scope.bucket_scope().bucket_size();
        this->resize(m_bucket_number);
    }

    void Reset() {
        this->clear();
        this->resize(m_bucket_number);
    }

    KeyType key(typename ShuffleContainer::iterator ptr) {
        return ptr - this->begin();
    }

    ValueType value(typename ShuffleContainer::iterator ptr) {
        return *ptr;
    }

private:
    uint32_t m_bucket_number;
};

}  // namespace internal
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_SHUFFLE_RUNNER_H_
