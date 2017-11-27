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
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#include "flume/runtime/common/single_dispatcher.h"

#include <cstring>

#include "boost/bind.hpp"
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

using core::Objector;

class SingleDispatcher::IteratorImpl : public core::Iterator {
public:
    explicit IteratorImpl(core::Objector* objector) : m_holder(objector), m_iterator(NULL) {}

    void SetDataset(Dataset* dataset) {
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

class SingleDispatcher::HandleImpl : public Source::Handle {
public:
    explicit HandleImpl(SingleDispatcher* base) : m_base(base) {}

    virtual void Done() {
        if (!m_base->m_need_more) {
            return;
        }
        m_base->m_need_more = false;

        if (m_base->m_iterator) {
            for (size_t i = m_base->m_dispatch_level; i < m_base->m_datasets.size(); ++i) {
                Dataset* dataset = m_base->m_datasets[i];
                if (!dataset->IsReady()) {
                    dataset->Discard()->Done();
                }
            }
        }
    }

private:
    SingleDispatcher* m_base;
};

class SingleDispatcher::SourceImpl : public Source {
public:
    SourceImpl(SingleDispatcher* base, uint32_t scope_level) :
            m_base(base), m_level(scope_level) {}

    virtual Handle* RequireStream(uint32_t flag,
                                  StreamCallback* callback, DoneCallback* done) {
        m_base->m_dispatch_level = m_level;
        m_base->m_dispatch_flag = flag;

        m_base->m_stream_callback.reset(callback);
        m_base->m_done_callback.reset(done);

        CHECK(m_base->m_handle == NULL);
        m_base->m_handle.reset(new HandleImpl(m_base));
        return m_base->m_handle.get();
    }

    virtual Handle* RequireIterator(IteratorCallback* iterator_callback,
                                    DoneCallback* fallback_done) {
        CHECK_NOTNULL(m_base->m_dataset_manager);

        m_base->m_dispatch_level = m_level;
        m_base->m_dispatch_flag = 0;
        m_base->m_iterator.reset(new IteratorImpl(m_base->m_objector.get()));

        m_base->m_iterator_callback.reset(iterator_callback);
        m_base->m_done_callback.reset(fallback_done);

        CHECK(m_base->m_handle == NULL);
        m_base->m_handle.reset(new HandleImpl(m_base));
        return m_base->m_handle.get();
    }

private:
    SingleDispatcher* m_base;
    uint32_t m_level;
};

SingleDispatcher::SingleDispatcher(const std::string& identity, uint32_t scope_level) :
        m_dataset_manager(NULL), m_buffer(2 * 1024 * 1024 /*2M*/),
        m_identity(identity), m_input_level(scope_level), m_handle(NULL),
        m_dispatch_level(0), m_dispatch_flag(0), m_need_more(false), m_retired_dataset(NULL) {
    for (uint32_t i = 0; i <= scope_level; ++i) {
        m_sources.push_back(new SourceImpl(this, i));
    }
}

SingleDispatcher::~SingleDispatcher() {
    ReleaseRetiredDataset();
}

void SingleDispatcher::SetDatasetManager(DatasetManager* dataset_manager) {
    m_dataset_manager = dataset_manager;
}

void SingleDispatcher::SetObjector(const PbEntity& message) {
    using core::Entity;
    using core::Objector;
    m_objector.reset(Entity<Objector>::From(message).CreateAndSetup());
}

Source* SingleDispatcher::GetSource(uint32_t scope_level) {
    return &m_sources[scope_level];
}

uint32_t SingleDispatcher::GetScopeLevel() {
    return m_keys.size() - 1;
}

Dataset* SingleDispatcher::GetDataset(uint32_t scope_level) {
    DCHECK_LT(scope_level, m_datasets.size());
    return m_datasets[scope_level];
}

bool SingleDispatcher::IsAcceptMore() {
    return m_need_more;
}

void SingleDispatcher::BeginGroup(const toft::StringPiece& key) {
    ReleaseRetiredDataset();

    Dataset* dataset = NULL;
    if (m_dataset_manager == NULL) {
    } else if (m_keys.size() == 0) {
        dataset = m_dataset_manager->GetDataset(m_identity);
    } else {
        dataset = m_datasets.back()->GetChild(key);
    }

    BeginGroup(key, dataset);
}

void SingleDispatcher::BeginGroup(const toft::StringPiece& key, Dataset* dataset) {
    ReleaseRetiredDataset(dataset);

    m_keys.push_back(key);
    if (current_level() == m_dispatch_level) {
        m_need_more = true;
    }

    if (dataset != NULL && !dataset->IsReady()) {
        if (!m_need_more || !m_iterator) {
            dataset->Discard()->Done();
        }
    }
    m_datasets.push_back(dataset);
}

void SingleDispatcher::FinishGroup() {
    ReleaseRetiredDataset();

    DCHECK_GT(m_keys.size(), 0);
    if (current_level() != m_input_level) {
        LeaveLastLevel();
        return;
    }

    Dataset* dataset = m_datasets.back();
    if (dataset != NULL && dataset->IsReady()) {
        if (!m_iterator) {
            FlushDataset(m_keys, dataset);
        }

        if (m_input_level == m_dispatch_level) {
            LeaveLastLevel();
        }
    }
}

// 当datase需要跨group复用时，不能在BeginGroup里调用dataset的release
// 一种应用场景是ShuffleExexutor的DistributeByEveryRecordRunner
inline void SingleDispatcher::ReleaseRetiredDataset(Dataset* dataset) {
    if (m_retired_dataset == NULL) {
        return;
    }

    if (m_iterator) {
        m_iterator->Done();
    }

    if (dataset != m_retired_dataset) {
        m_retired_dataset->Release();
    }
    m_retired_dataset = NULL;
}

inline void SingleDispatcher::LeaveLastLevel() {
    // still need cache
    Dataset* dataset = m_datasets.back();
    if (m_iterator && m_need_more && !dataset->IsReady()) {
        dataset->Commit();
    }

    if (current_level() == m_dispatch_level) {
        if (m_iterator && m_need_more) {
            m_iterator->SetDataset(dataset);
            m_iterator_callback->Run(m_iterator.get());
        }
        m_done_callback->Run();

        m_need_more = false;
    }
    m_keys.pop_back();

    m_retired_dataset = m_datasets.back();
    m_datasets.pop_back();
}

inline void SingleDispatcher::FlushDataset(const std::vector<toft::StringPiece>& keys,
                                           Dataset* dataset) {
    Dataset::Iterator* iterator =
            dataset->IsReady() ? dataset->NewIterator() : dataset->Discard();
    while (m_need_more && iterator->HasNext()) {
        toft::StringPiece binary = iterator->NextValue();

        ObjectHolder holder(m_objector.get());
        void *object = is_require_object() ? holder.Reset(binary) : NULL;

        m_stream_callback->Run(keys, object, binary);
    }
    iterator->Done();
}

bool SingleDispatcher::EmitObject(void* object) {
    return SendObject(m_keys, object);
}

bool SingleDispatcher::EmitObject(const std::vector<toft::StringPiece>& keys, void* object) {
    return SendObject(keys, object);
}

bool SingleDispatcher::EmitBinary(const toft::StringPiece& binary) {
    return SendBinary(m_keys, binary);
}

bool SingleDispatcher::EmitBinary(const std::vector<toft::StringPiece>& keys,
                                  const toft::StringPiece& binary) {
    return SendBinary(keys, binary);
}

void SingleDispatcher::Done() {
    if (current_level() == m_input_level) {
        LeaveLastLevel();
    }
}

inline bool SingleDispatcher::SendObject(const std::vector<toft::StringPiece>& keys,
                                         void* object) {
    DCHECK_EQ(current_level(), m_input_level);
    if (!m_need_more) {
        return false;
    }

    if (m_iterator) {
        Dataset* dataset = m_datasets.back();
        util::Arena* arena = dataset->AcquireArena();

        dataset->Emit(
            arena->Allocate(boost::bind(&Objector::Serialize, m_objector.get(), object, _1, _2))
        );
        dataset->ReleaseArena();
    } else {
        SerializeBuffer::Guard buffer_guard;
        if (is_require_binary()) {
            buffer_guard = m_buffer.Allocate(m_objector.get(), &Objector::Serialize, object);
        }

        m_stream_callback->Run(keys, object, buffer_guard.ref());
    }

    return m_need_more;
}

inline bool SingleDispatcher::SendBinary(const std::vector<toft::StringPiece>& keys,
                                         const toft::StringPiece& binary) {
    DCHECK_EQ(current_level(), m_input_level);
    if (!m_need_more) {
        return false;
    }

    if (m_iterator) {
        Dataset* dataset = m_datasets.back();
        util::Arena* arena = dataset->AcquireArena();
        dataset->Emit(arena->Allocate(binary));
        dataset->ReleaseArena();
    } else {
        ObjectHolder holder(m_objector.get());

        void* object = NULL;
        if (is_require_object()) {
            object = holder.Reset(binary);
        }

        m_stream_callback->Run(keys, object, binary);
    }

    return m_need_more;
}

class SingleStreamDispatcher::HandleImpl : public Source::Handle {
public:
    explicit HandleImpl(SingleStreamDispatcher* base) : m_base(base) {}

    virtual void Done() {
        m_base->m_need_more = false;
    }

private:
    SingleStreamDispatcher* m_base;
};

class SingleStreamDispatcher::SourceImpl : public Source {
public:
    SourceImpl(SingleStreamDispatcher* base, uint32_t scope_level) :
            m_base(base), m_level(scope_level) {}

    virtual Handle* RequireStream(uint32_t flag,
                                  StreamCallback* callback, DoneCallback* done) {
        m_base->m_dispatch_level = m_level;
        m_base->m_dispatch_flag = flag;

        m_base->m_stream_callback.reset(callback);
        m_base->m_done_callback.reset(done);

        CHECK(m_base->m_handle == NULL);
        m_base->m_handle.reset(new HandleImpl(m_base));
        return m_base->m_handle.get();
    }

    virtual Handle* RequireIterator(IteratorCallback* iterator_callback,
                                    DoneCallback* fallback_done) {
        LOG(FATAL) << "SingleStreamDispatcher do not support iterator listener!";
        return NULL;
    }

private:
    SingleStreamDispatcher* m_base;
    uint32_t m_level;
};

SingleStreamDispatcher::SingleStreamDispatcher(const std::string& identity,
                                               uint32_t scope_level) :
        m_buffer(2 * 1024 * 1024 /*2M*/), m_identity(identity),
        m_input_level(scope_level), m_dispatch_level(0), m_dispatch_flag(0),
        m_need_more(false) {
    for (uint32_t i = 0; i <= scope_level; ++i) {
        m_sources.push_back(new SourceImpl(this, i));
    }
}

SingleStreamDispatcher::~SingleStreamDispatcher() {}

void SingleStreamDispatcher::SetDatasetManager(DatasetManager* dataset_manager) {
    LOG(WARNING) << "SingleStreamDispatcher does not need DatasetManager!";
}

void SingleStreamDispatcher::SetObjector(const PbEntity& message) {
    using core::Entity;
    using core::Objector;
    m_objector.reset(Entity<Objector>::From(message).CreateAndSetup());
}

Source* SingleStreamDispatcher::GetSource(uint32_t scope_level) {
    return &m_sources[scope_level];
}

uint32_t SingleStreamDispatcher::GetScopeLevel() {
    return m_keys.size() - 1;
}

Dataset* SingleStreamDispatcher::GetDataset(uint32_t scope_level) {
    LOG(FATAL) << "SingleStreamDispatcher will not provide dataset";
    return NULL;
}

bool SingleStreamDispatcher::IsAcceptMore() {
    return m_need_more;
}

void SingleStreamDispatcher::BeginGroup(const toft::StringPiece& key) {
    m_keys.push_back(key);
    if (current_level() == m_dispatch_level) {
        m_need_more = true;
    }
}

void SingleStreamDispatcher::BeginGroup(const toft::StringPiece& key, Dataset* dataset) {
    LOG(FATAL) << "SingleStreamDispatcher do not accept Dataset!";
}

void SingleStreamDispatcher::FinishGroup() {
    DCHECK_GT(m_keys.size(), 0);
    if (current_level() != m_input_level) {
        LeaveLastLevel();
        return;
    }
}

inline void SingleStreamDispatcher::LeaveLastLevel() {
    if (current_level() == m_dispatch_level) {
        m_done_callback->Run();
        m_need_more = false;
    }
    m_keys.pop_back();
}

bool SingleStreamDispatcher::EmitObject(void* object) {
    return SendObject(m_keys, object);
}

bool SingleStreamDispatcher::EmitObject(const std::vector<toft::StringPiece>& keys, void* object) {
    return SendObject(keys, object);
}

bool SingleStreamDispatcher::EmitBinary(const toft::StringPiece& binary) {
    return SendBinary(m_keys, binary);
}

bool SingleStreamDispatcher::EmitBinary(const std::vector<toft::StringPiece>& keys,
                                        const toft::StringPiece& binary) {
    return SendBinary(keys, binary);
}

void SingleStreamDispatcher::Done() {
    if (current_level() == m_input_level) {
        LeaveLastLevel();
    }
}

inline bool SingleStreamDispatcher::SendObject(const std::vector<toft::StringPiece>& keys,
                                               void* object) {
    DCHECK_EQ(current_level(), m_input_level);
    if (!m_need_more) {
        return false;
    }

    SerializeBuffer::Guard buffer_guard;
    if (is_require_binary()) {
        buffer_guard = m_buffer.Allocate(m_objector.get(), &Objector::Serialize, object);
    }

    m_stream_callback->Run(keys, object, buffer_guard.ref());

    return m_need_more;
}

inline bool SingleStreamDispatcher::SendBinary(const std::vector<toft::StringPiece>& keys,
                                               const toft::StringPiece& binary) {
    DCHECK_EQ(current_level(), m_input_level);
    if (!m_need_more) {
        return false;
    }

    ObjectHolder holder(m_objector.get());
    void* object = NULL;
    if (is_require_object()) {
        object = holder.Reset(binary);
    }
    m_stream_callback->Run(keys, object, binary);

    return m_need_more;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
