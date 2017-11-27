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

#include "flume/runtime/common/local_dataset.h"

#include <arpa/inet.h>
#include <algorithm>
#include <iterator>
#include <map>
#include <memory>

#include "boost/algorithm/hex.hpp"
#include "boost/foreach.hpp"
#include "boost/tuple/tuple.hpp"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include "flume/util/arena.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace {

class EmptyIterator : public Dataset::Iterator {
public:
    virtual bool HasNext() const {
        return false;
    }

    virtual toft::StringPiece NextValue() {
        LOG(FATAL) << "no next value";
        return toft::StringPiece();
    }

    virtual void Reset() {}

    virtual void Done() {}
};

EmptyIterator kEmptyIterator;

}  // namespace

class LocalDatasetManager::DatasetImpl::MemoryChunkIterator : public Dataset::Iterator {
public:
    // passed in chunk should contains at least one record
    MemoryChunkIterator* Initialize(MemoryRecord* first, MemoryRecord* last) {
        m_first = first;
        m_last = last;
        m_next = first;
        return this;
    }

    virtual bool HasNext() const {
        return m_next != NULL;
    }

    virtual toft::StringPiece NextValue() {
        toft::StringPiece value = m_next->data;
        if (m_next == m_last) {
            m_next = NULL;
        } else {
            MemoryChunk::iterator next_ptr = ++MemoryChunk::s_iterator_to(*m_next);
            m_next = &*next_ptr;
        }
        return value;
    }

    virtual void Reset() {
        m_next = m_first;
    }

    virtual void Done() {
    }

private:
    MemoryRecord* m_first;
    MemoryRecord* m_last;
    MemoryRecord* m_next;
};

class LocalDatasetManager::DatasetImpl::DiskChunkIterator : public Dataset::Iterator {
public:
    DiskChunkIterator(leveldb::DB* db, const leveldb::Slice& anchor, uint64_t count) :
            m_iterator(db->NewIterator(leveldb::ReadOptions())),
            m_anchor(anchor.ToString()), m_offset(0), m_count(count) {}

    virtual bool HasNext() const {
        return m_offset < m_count;
    }

    virtual toft::StringPiece NextValue() {
        if (m_offset == 0) {
            m_iterator->Seek(m_anchor);
        } else {
            m_iterator->Next();
        }

        ++m_offset;
        return toft::StringPiece(m_iterator->value().data(), m_iterator->value().size());
    }

    virtual void Reset() {
        m_offset = 0;
    }

    virtual void Done() {
        m_iterator.reset();
    }

private:
    boost::scoped_ptr<leveldb::Iterator> m_iterator;
    std::string m_anchor;
    uint64_t m_offset;
    uint64_t m_count;
};

LocalDatasetManager::LocalDatasetManager() : m_max_used_bytes(0), m_next_sequence(0) {}

LocalDatasetManager::~LocalDatasetManager() {}

LocalDatasetManager::DatasetImpl::DatasetImpl(LocalDatasetManager* manager) :
        m_manager(manager), m_father(NULL), m_ref(0), m_state(RELEASED),
        m_record_count(0), m_first_memory_record(NULL), m_last_memory_record(NULL) {}

LocalDatasetManager::DatasetImpl::DatasetImpl(
    const LocalDatasetManager::DatasetImpl& impl) :
        m_manager(impl.m_manager), m_father(impl.m_father), m_ref(impl.m_ref),
        m_state(impl.m_state), m_record_count(impl.m_record_count),
        m_first_memory_record(impl.m_first_memory_record),
        m_last_memory_record(impl.m_last_memory_record) {}

void LocalDatasetManager::Initialize(const std::string& swap_dir, uint64_t max_used_bytes) {
    m_max_used_bytes = max_used_bytes;
    m_arena.reset(new util::Arena);
    system(("rm -rf " + swap_dir).c_str());
    leveldb::DB* db = NULL;
    leveldb::Options options;
    // The default value of this option is 1000. Here we set it to a smaller value
    // to reduce memory usage.
    options.max_open_files = 32;
    options.create_if_missing = true;
    options.error_if_exists = true;
    leveldb::Status status = leveldb::DB::Open(options, swap_dir, &db);
    CHECK(status.ok()) << "swap_dir:" << swap_dir << ", reason:" << status.ToString();
    m_db.reset(db);
}

LocalDatasetManager::DatasetImpl* LocalDatasetManager::GetDataset(const std::string& identity) {
    return AllocateDataset(NULL);
}

LocalDatasetManager::DatasetImpl* LocalDatasetManager::AllocateDataset(DatasetImpl* father) {
    DatasetImpl* dataset = NULL;
    if (m_released_datasets.empty()) {

#if __GNUC__ == 3
        m_dataset_pool.push_back(DatasetImpl(this));
#else
        m_dataset_pool.emplace_back(this);
#endif // #if __GNUC__ == 3

        dataset = &m_dataset_pool.back();
    } else {
        dataset = &m_released_datasets.back();
        m_released_datasets.pop_back();
    }

    dataset->Initialize(m_next_sequence++, father);
    return dataset;
}


#if __GNUC__ == 3
namespace {

uint64_t htobe64(uint64_t v) {
    union { uint32_t lv[2]; uint64_t llv; } u;
    u.lv[0] = htonl(v >> 32);
    u.lv[1] = htonl(v & 0xFFFFFFFFULL);
    return u.llv;
}

} // namespace

#endif

void LocalDatasetManager::DatasetImpl::Initialize(uint64_t sequence, DatasetImpl* father) {

    if (father != NULL) {
        father->m_ref++;
    }

    m_father = father;
    m_ref = 1;
    m_state = NORMAL;
    m_record_count = 0;

    m_memory_arena = m_manager->m_arena;
    m_manager->m_memory_datasets.push_back(*this);

    m_dumping_key[0] = htobe64(sequence);
}

void LocalDatasetManager::DatasetImpl::AddReference() {
    ++m_ref;
}

void LocalDatasetManager::DatasetImpl::Release() {
    DCHECK_GT(m_ref, 0);
    --m_ref;
    if (m_ref > 0) {
        return;
    }

    if (m_father != NULL) {
        m_father->Release();
        m_father = NULL;
    }

    m_memory_chunk.clear();
    m_first_memory_record = NULL;
    m_last_memory_record = NULL;

    m_temporary_arena.reset();
    m_memory_arena.reset();
    m_disk_iterators.clear();

    this->unlink();
    m_manager->m_released_datasets.push_back(*this);
    m_state = RELEASED;

    m_manager->CheckMemoryUsage();
}

LocalDatasetManager::DatasetImpl*
LocalDatasetManager::DatasetImpl::GetChild(const toft::StringPiece& key) {
    return m_manager->AllocateDataset(this);
}

bool LocalDatasetManager::DatasetImpl::IsReady() {
    return m_state == READY;
}

void LocalDatasetManager::DatasetImpl::Emit(const toft::StringPiece& value) {
    DCHECK_EQ(m_state, NORMAL);

    if (m_memory_arena != NULL) {
        MemoryRecord* record = m_memory_arena->Allocate<MemoryRecord>();
        record->data = value;
        m_memory_chunk.push_back(*record);
    } else {
        m_manager->m_db->Put(leveldb::WriteOptions(), AsKey(m_record_count), AsValue(value));
    }

    ++m_record_count;
}

void LocalDatasetManager::DatasetImpl::Commit() {
    DCHECK_EQ(m_state, NORMAL);

    this->unlink();
    m_manager->m_final_datasets.push_back(*this);
    m_state = READY;

    if (m_record_count == 0) {
        m_memory_arena.reset();
        return;
    }

    if (m_memory_arena != NULL) {
        m_first_memory_record = &m_memory_chunk.front();
        m_last_memory_record = &m_memory_chunk.back();
        if (m_father != NULL) {
            m_father->Append(&m_memory_chunk);
        }
    } else {
        if (m_father != NULL) {
            m_father->Append(AsKey(0), m_record_count);
        }
    }
}

void LocalDatasetManager::DatasetImpl::Append(MemoryChunk* chunk) {
    if (m_state != NORMAL) {
        return;
    }

    if (m_memory_arena != NULL) {
        m_record_count += chunk->size();
        m_memory_chunk.splice_after(m_memory_chunk.last(), *chunk);
    } else {
        leveldb::DB* db = m_manager->m_db.get();
        for (MemoryChunk::iterator ptr = chunk->begin(); ptr != chunk->end(); ++ptr) {
            db->Put(leveldb::WriteOptions(), AsKey(m_record_count++), AsValue(ptr->data));
        }
    }
}

void LocalDatasetManager::DatasetImpl::Append(const leveldb::Slice& anchor, uint64_t size) {
    if (m_state != NORMAL) {
        return;
    }

    leveldb::DB* db = m_manager->m_db.get();
    boost::scoped_ptr<leveldb::Iterator> iterator(db->NewIterator(leveldb::ReadOptions()));
    iterator->Seek(anchor);
    for (uint64_t i = 0; i < size; ++i) {
        db->Put(leveldb::WriteOptions(), AsKey(m_record_count++), iterator->value());
        iterator->Next();
    }
}

Dataset::Iterator* LocalDatasetManager::DatasetImpl::Discard() {
    DCHECK_EQ(m_state, NORMAL);

    this->unlink();
    m_manager->m_final_datasets.push_back(*this);
    m_state = DISCARDED;

    if (m_record_count == 0) {
        m_memory_arena.reset();
    } else if (m_memory_arena != NULL) {
        m_first_memory_record = &m_memory_chunk.front();
        m_last_memory_record = &m_memory_chunk.back();
    }
    Dataset::Iterator* iterator = NewIterator();

    if (m_father != NULL) {
        m_father->Release();
    }
    m_father = NULL;

    return iterator;
}

Dataset::Iterator* LocalDatasetManager::DatasetImpl::NewIterator() {
    DCHECK(m_state == READY || m_state == DISCARDED);

    if (m_record_count == 0) {
        // empty dataset
        return &kEmptyIterator;
    }

    if (m_memory_arena != NULL) {
        MemoryChunkIterator* iterator = m_memory_arena->Allocate<MemoryChunkIterator>();
        return iterator->Initialize(m_first_memory_record, m_last_memory_record);
    } else {
        m_disk_iterators.push_back(
                new DiskChunkIterator(m_manager->m_db.get(), AsKey(0), m_record_count));
        return &m_disk_iterators.back();
    }

    return NULL;
}

util::Arena* LocalDatasetManager::DatasetImpl::AcquireArena() {
    m_temporary_arena = m_memory_arena != NULL ? m_memory_arena : m_manager->m_arena;
    return m_temporary_arena.get();
}

void LocalDatasetManager::DatasetImpl::ReleaseArena() {
    m_temporary_arena.reset();
    m_manager->CheckMemoryUsage();
}

void LocalDatasetManager::DatasetImpl::Dump() {
    DCHECK_EQ(m_state, NORMAL);
    DCHECK(m_memory_arena != NULL);

    uint64_t index = 0;
    leveldb::DB* db = m_manager->m_db.get();
    for (MemoryChunk::iterator ptr = m_memory_chunk.begin(); ptr != m_memory_chunk.end(); ++ptr) {
        db->Put(leveldb::WriteOptions(), AsKey(index++), AsValue(ptr->data));
    }
    DCHECK_EQ(index, m_record_count);

    this->unlink();
    m_manager->m_disk_datasets.push_back(*this);
    m_memory_arena.reset();
}

void LocalDatasetManager::CheckMemoryUsage() {
    if (m_arena->total_reserved_bytes() > m_max_used_bytes / 2 && m_arena.use_count() == 1) {
        LOG(INFO) << "cur_usage = " << m_arena->total_reserved_bytes()
            << "/" << m_max_used_bytes <<" bytes";
        LOG(INFO) << "Shrinking memory usage ... ";

        m_arena->Reset();
    }

    if (m_arena->total_reserved_bytes() > m_max_used_bytes) {
        LOG(INFO) << "cur_usage = " << m_arena->total_reserved_bytes()
            << "/" << m_max_used_bytes <<" bytes";

        LOG(INFO) << "Dumping datas into disk ...";
        while (!m_memory_datasets.empty()) {
            m_memory_datasets.front().Dump();  // Dump will remove dataset from m_memory_datasets
        }
        m_arena.reset(new util::Arena);
    }
}

leveldb::Slice LocalDatasetManager::DatasetImpl::AsKey(uint64_t offset) {
    m_dumping_key[1] = htobe64(offset);
    return leveldb::Slice(reinterpret_cast<char*>(&m_dumping_key), sizeof(m_dumping_key));
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
