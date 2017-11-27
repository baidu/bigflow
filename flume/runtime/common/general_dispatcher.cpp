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
// Author: Wen Xiang <wenxiang@baidu.com>

#include "flume/runtime/common/general_dispatcher.h"

#include <cstring>

#include "boost/dynamic_bitset.hpp"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/emitter.h"
#include "flume/core/entity.h"
#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/runtime/util/object_holder.h"
#include "flume/util/arena.h"

namespace baidu {
namespace flume {
namespace runtime {

class GeneralDispatcher::IteratorImpl : public core::Iterator {
public:
    explicit IteratorImpl(core::Objector* objector) : m_holder(objector), m_iterator(NULL) {}

    void Setup(Dataset* dataset) {
        CHECK(m_iterator == NULL);
        m_iterator = dataset->NewIterator();
    }

    virtual bool HasNext() const {
        return m_iterator->HasNext();
    }

    virtual void* NextValue() {
        m_holder.Clear();
        return m_holder.Reset(m_iterator->NextValue());
    }

    virtual void Reset() {
        m_holder.Clear();
        m_iterator->Reset();
    }

    virtual void Done() {
        m_holder.Clear();

        if (m_iterator != NULL) {
            m_iterator->Done();
            m_iterator = NULL;
        }
    }

private:
    ObjectHolder m_holder;
    Dataset::Iterator* m_iterator;
};

class GeneralDispatcher::HandleImpl : public Source::Handle {
public:
    HandleImpl(uint32_t handle, uint32_t level, GeneralDispatcher* base)
        : m_handle(handle), m_level(level), m_base(base) {}

    virtual void Done() {
        if (m_level >= m_base->m_runtimes.size()) {
            return;
        }

        ScopeLevelRuntime& runtime = m_base->m_runtimes[m_level];
        runtime.done_bitset[m_handle] = true;

        m_base->m_object_bitset[m_handle] = false;
        m_base->m_binary_bitset[m_handle] = false;
        m_base->m_stream_bitset[m_handle] = false;
    }

private:
    unsigned m_handle;
    unsigned m_level;
    GeneralDispatcher* m_base;
};

class GeneralDispatcher::IteratorHandleImpl : public Source::Handle {
public:
    IteratorHandleImpl(uint32_t handle, uint32_t level, GeneralDispatcher* base)
        : m_handle(handle), m_level(level), m_base(base) {}

    virtual void Done() {
        if (m_level >= m_base->m_runtimes.size()) {
            return;
        }

        ScopeLevelRuntime& runtime = m_base->m_runtimes[m_level];
        if (runtime.done_bitset[m_handle]) {
            return;
        }
        runtime.done_bitset[m_handle] = true;
        runtime.iterator_bitset[m_handle] = false;

        m_base->m_cache_bitset[m_handle] = false;
    }

private:
    uint32_t m_handle;
    uint32_t m_level;
    GeneralDispatcher* m_base;
};

class GeneralDispatcher::SourceImpl : public Source {
public:
    SourceImpl(GeneralDispatcher* base, uint32_t scope_level)
            : m_base(base), m_level(scope_level) {}

    virtual Handle* RequireStream(uint32_t flag,
                                  StreamCallback* callback, DoneCallback* done) {
        m_callbacks.push_back(callback);
        m_callbacks.push_back(done);

        return CreateStreamHandle(flag & REQUIRE_OBJECT, flag & REQUIRE_BINARY, callback, done);
    }

    virtual Handle* RequireIterator(IteratorCallback* iterator_callback, DoneCallback* done) {
        m_callbacks.push_back(iterator_callback);
        m_callbacks.push_back(done);

        return CreateIteratorHandle(iterator_callback, done);
    }

private:
    Handle* CreateStreamHandle(bool is_require_object, bool is_require_binary,
                               StreamCallback* callback, DoneCallback* done) {
        uint32_t handle = m_base->AllocateHandle(m_level);

        Source::Handle* result = new HandleImpl(handle, m_level, m_base);
        m_handles.push_back(result);

        m_base->m_stream_callbacks[handle] = callback;
        m_base->m_done_callbacks[handle] = done;
        m_base->m_configs[m_level].object_bitset[handle] = is_require_object;
        m_base->m_configs[m_level].binary_bitset[handle] = is_require_binary;

        return result;
    }

    Handle* CreateIteratorHandle(IteratorCallback* iterator_callback, DoneCallback* done) {
        uint32_t handle = m_base->AllocateHandle(m_level);

        IteratorHandleImpl* result = new IteratorHandleImpl(handle, m_level, m_base);
        m_handles.push_back(result);

        m_base->m_iterator_callbacks[handle] = iterator_callback;
        m_base->m_done_callbacks[handle] = done;
        m_base->m_configs[m_level].iterator_bitset[handle] = true;

        return result;
    }

    GeneralDispatcher* m_base;
    uint32_t m_level;

    // deleters
    boost::ptr_vector<toft::ClosureBase> m_callbacks;
    boost::ptr_vector<Source::Handle> m_handles;
};

GeneralDispatcher::GeneralDispatcher(const std::string& identity, uint32_t scope_level)
        : m_dataset_manager(NULL), m_objector(NULL),
          m_identity(identity), m_handle_count(0),
          m_last_dataset(NULL), m_buffer(new char[kBufferSize]) {
    for (uint32_t i = 0; i <= scope_level; ++i) {
        m_sources.push_back(new SourceImpl(this, i));
    }
    m_configs.resize(scope_level + 1);
}

GeneralDispatcher::~GeneralDispatcher() {
    ReleaseLastDataset();
}

void GeneralDispatcher::SetObjector(const PbEntity& message) {
    using core::Entity;
    using core::Objector;
    m_objector.reset(Entity<Objector>::From(message).CreateAndSetup());
}

void GeneralDispatcher::SetDatasetManager(DatasetManager* dataset_manager) {
    m_dataset_manager = dataset_manager;
}

Source* GeneralDispatcher::GetSource(uint32_t scope_level) {
    return &m_sources[scope_level];
}

uint32_t GeneralDispatcher::AllocateHandle(uint32_t scope_level) {
    uint32_t handle = m_handle_count++;

    for (size_t i = 0; i < m_configs.size(); ++i) {
        ScopeLevelConfig& config = m_configs[i];

        config.zero_mask.push_back(false);
        if (i == scope_level) {
            config.set_mask.push_back(true);
            config.clear_mask.push_back(false);
        } else {
            config.clear_mask.push_back(true);
            config.set_mask.push_back(false);
        }
        config.object_bitset.push_back(false);
        config.binary_bitset.push_back(false);
        config.iterator_bitset.push_back(false);
    }

    m_stream_callbacks.push_back(NULL);
    m_done_callbacks.push_back(NULL);
    m_iterator_callbacks.push_back(NULL);
    m_iterators.push_back(new IteratorImpl(m_objector.get()));

    m_object_bitset.push_back(false);
    m_binary_bitset.push_back(false);
    m_stream_bitset.push_back(false);
    m_cache_bitset.push_back(false);

    return handle;
}

void GeneralDispatcher::BeginGroup(const toft::StringPiece& key) {
    ReleaseLastDataset();

    Dataset* dataset = NULL;
    if (m_dataset_manager == NULL) {
        CHECK(m_cache_bitset.none());
    } else if (m_cache_bitset.none()
                && m_runtimes.size() + 1 == m_configs.size()
                && m_configs.back().iterator_bitset.none()) {
        // no need for dataset
    } else if (m_runtimes.size() == 0) {
        dataset = m_dataset_manager->GetDataset(m_identity);
    } else {
        dataset = m_runtimes.back().dataset->GetChild(key);
    }

    BeginGroup(key, dataset);
}

void GeneralDispatcher::BeginGroup(const toft::StringPiece& key, Dataset* dataset) {
    ReleaseLastDataset();

    DCHECK_LT(m_runtimes.size(), m_configs.size());
    uint32_t next_level = m_runtimes.size();
    ScopeLevelConfig& config = m_configs[next_level];

    m_stream_bitset |= config.set_mask;
    m_stream_bitset ^= config.iterator_bitset;

    m_object_bitset |= config.object_bitset;
    m_binary_bitset |= config.binary_bitset;
    m_cache_bitset |= config.iterator_bitset;

    ScopeLevelRuntime runtime;
    runtime.iterator_bitset = config.iterator_bitset;
    runtime.done_bitset = config.zero_mask;
    runtime.dataset = dataset;
    m_runtimes.push_back(runtime);
    m_keys.push_back(key);

    if (m_cache_bitset.none() && dataset != NULL && !dataset->IsReady()) {
        dataset->Discard()->Done();
    }
}

void GeneralDispatcher::FinishGroup() {
    ReleaseLastDataset();

    DCHECK_GT(m_runtimes.size(), 0);
    if (m_runtimes.size() != m_configs.size()) {
        RetireLastRuntime();
    } else {
        Dataset* dataset = m_runtimes.back().dataset;
        if (dataset != NULL && dataset->IsReady()) {
            FlushDataset(m_stream_bitset, m_object_bitset, m_keys, dataset->NewIterator());
        }
    }
}

uint32_t GeneralDispatcher::GetScopeLevel() {
    return m_runtimes.size() - 1;
}

Dataset* GeneralDispatcher::GetDataset(uint32_t scope_level) {
    return m_runtimes[scope_level].dataset;
}

// 当datase需要跨group复用时，不能在BeginGroup里调用dataset的release
// 一种应用场景是ShuffleExexutor的DistributeByEveryRecordRunner
inline void GeneralDispatcher::ReleaseLastDataset() {
    if (m_last_dataset != NULL) {
        for (size_t i = 0; i < m_iterators.size(); ++i) {
            m_iterators[i].Done();
        }

        m_last_dataset->Release();
        m_last_dataset = NULL;
    }
}

inline void GeneralDispatcher::RetireLastRuntime() {
    DCHECK_GT(m_runtimes.size(), 0u);
    uint32_t level = m_runtimes.size() - 1;
    const ScopeLevelConfig& config = m_configs[level];

    // commit dataset
    const ScopeLevelRuntime& runtime = m_runtimes.back();
    Dataset* dataset = runtime.dataset;
    if (m_cache_bitset.any() || runtime.iterator_bitset.any()) {
        if (!dataset->IsReady()) {
            dataset->Commit();
        }

        if (runtime.iterator_bitset.any()) {
            DispatchIterator(runtime.iterator_bitset, dataset);
        }
    }

    // call done for all handles at this level
    DispatchDone(config.set_mask);

    // retire runtime
    m_last_dataset = runtime.dataset;
    m_runtimes.pop_back();
    m_keys.pop_back();

    m_object_bitset &= config.clear_mask;
    m_binary_bitset &= config.clear_mask;
    m_stream_bitset &= config.clear_mask;
    m_cache_bitset &= config.clear_mask;
}

inline void GeneralDispatcher::FlushDataset(const Bitset& stream_bitset,
                                            const Bitset& object_bitset,
                                            const std::vector<toft::StringPiece>& keys,
                                            Dataset::Iterator* iterator) {
    while (stream_bitset.any() && iterator->HasNext()) {
        toft::StringPiece binary = iterator->NextValue();

        ObjectHolder holder(m_objector.get());
        void* object = object_bitset.any() ? holder.Reset(binary) : NULL;

        DispatchStream(stream_bitset, keys, object, binary);
    }
    iterator->Done();
}

inline void GeneralDispatcher::DispatchStream(const Bitset& bitset,
                                              const std::vector<toft::StringPiece>& keys,
                                              void* object, const toft::StringPiece& binary) {
    Bitset::size_type handle = bitset.find_first();
    while (handle < m_handle_count) {
        m_stream_callbacks[handle]->Run(keys, object, binary);
        handle = bitset.find_next(handle);
    }
}

inline void GeneralDispatcher::DispatchIterator(const Bitset& bitset, Dataset* dataset) {
    Bitset::size_type handle = bitset.find_first();
    while (handle < bitset.size()) {
        m_iterators[handle].Setup(dataset);
        m_iterator_callbacks[handle]->Run(&m_iterators[handle]);
        handle = bitset.find_next(handle);
    }
}

inline void GeneralDispatcher::DispatchDone(const Bitset& bitset) {
    Bitset::size_type handle = bitset.find_first();
    while (handle < bitset.size()) {
        m_done_callbacks[handle]->Run();
        handle = bitset.find_next(handle);
    }
}

bool GeneralDispatcher::EmitObject(void* object) {
    return SendObject(m_keys, object);
}

bool GeneralDispatcher::EmitObject(const std::vector<toft::StringPiece>& keys, void* object) {
    return SendObject(keys, object);
}

bool GeneralDispatcher::EmitBinary(const toft::StringPiece& binary) {
    return SendBinary(m_keys, binary);
}

bool GeneralDispatcher::EmitBinary(const std::vector<toft::StringPiece>& keys,
                                   const toft::StringPiece& binary) {
    return SendBinary(keys, binary);
}

inline bool GeneralDispatcher::SendObject(const std::vector<toft::StringPiece>& keys,
                                          void *object) {
    if (!CheckForLastScope()) {
        return false;
    }

    Dataset* dataset = m_runtimes.back().dataset;
    toft::scoped_array<char> temporary_buffer;

    util::Arena* arena = NULL;
    toft::StringPiece binary;
    if (m_cache_bitset.any() || m_binary_bitset.any()) {
        arena = m_cache_bitset.any() ? dataset->AcquireArena() : NULL;
        char* buffer = m_cache_bitset.any() ? arena->remained_buffer() : m_buffer.get();
        size_t buffer_size = m_cache_bitset.any() ? arena->remained_buffer_size() : kBufferSize;

        size_t data_size = m_objector->Serialize(object, buffer, buffer_size);
        if (data_size > buffer_size) {
            if (m_cache_bitset.any()) {
                arena->ReserveBytes(data_size);
                buffer = arena->remained_buffer();
                buffer_size = arena->remained_buffer_size();
            } else {
                temporary_buffer.reset(new char[data_size]);
                buffer = temporary_buffer.get();
                buffer_size = data_size;
            }

            data_size = m_objector->Serialize(object, buffer, buffer_size);
            DCHECK_LE(data_size, buffer_size);
        }

        if (m_cache_bitset.any()) {
            CHECK_EQ(buffer, arena->AllocateBytes(data_size));
        }
        binary.set(buffer, data_size);
    }

    DispatchStream(m_stream_bitset, keys, object, binary);
    if (m_cache_bitset.any()) {
        dataset->Emit(binary);
    }

    if (dataset != NULL && arena != NULL) {
        dataset->ReleaseArena();
    }

    return HasDownstreams();
}

inline bool GeneralDispatcher::SendBinary(const std::vector<toft::StringPiece>& keys,
                                          const toft::StringPiece& binary) {
    if (!CheckForLastScope()) {
        return false;
    }

    ObjectHolder holder(m_objector.get());
    void* object = m_object_bitset.any() ? holder.Reset(binary) : NULL;

    DispatchStream(m_stream_bitset, keys, object, binary);
    if (m_cache_bitset.any()) {
        Dataset* dataset = m_runtimes.back().dataset;

        char* buffer = dataset->AcquireArena()->AllocateBytes(binary.size());
        std::memcpy(buffer, binary.data(), binary.size());

        dataset->Emit(toft::StringPiece(buffer, binary.size()));
        dataset->ReleaseArena();
    }

    return HasDownstreams();
}

void GeneralDispatcher::Done() {
    if (m_runtimes.size() == m_configs.size()) {
        CheckForLastScope();
        RetireLastRuntime();
    }
}

bool GeneralDispatcher::IsAcceptMore() {
    if (m_runtimes.size() < m_configs.size()) {
        return true;
    }

    Dataset* dataset = m_runtimes.back().dataset;
    if (dataset != NULL && dataset->IsReady()) {
        return false;
    }

    return HasDownstreams();
}

inline bool GeneralDispatcher::CheckForLastScope() {
    DCHECK_EQ(m_runtimes.size(), m_configs.size());

    Dataset* dataset = m_runtimes.back().dataset;
    if (dataset != NULL && dataset->IsReady()) {
        return false;
    }

    return HasDownstreams();
}

inline bool GeneralDispatcher::HasDownstreams() {
    return m_stream_bitset.any() || m_cache_bitset.any();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
