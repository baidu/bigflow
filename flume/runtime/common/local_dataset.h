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
// Author: Wen Xiang <wenxiang@baidu.com>
//
// An implemention of Dataset.

#ifndef FLUME_RUNTIME_COMMON_LOCAL_DATASET_H_
#define FLUME_RUNTIME_COMMON_LOCAL_DATASET_H_

#include <arpa/inet.h>

#include <deque>
#include <list>
#include <string>
#include <utility>
#include <vector>

#include "flume/runtime/dataset.h"

#include "toft/base/portable_byte_order.h"
#include "boost/container/deque.hpp"
#include "boost/intrusive/list.hpp"
#include "boost/intrusive/slist.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/scoped_array.hpp"
#include "boost/scoped_ptr.hpp"
#include "boost/shared_ptr.hpp"
#include "boost/unordered_map.hpp"
#include "boost/weak_ptr.hpp"
#include "leveldb/db.h"

#include "flume/util/arena.h"
#include "flume/util/intrusive_ptr_base.h"
#include "flume/util/reusable_object_pool.h"

namespace leveldb {
class DB;
};

namespace baidu {
namespace flume {
namespace runtime {

class LocalDatasetManager : public DatasetManager {
public:
    typedef boost::shared_ptr<util::Arena> SharedArena;

    class DatasetImpl : public Dataset, public boost::intrusive::list_base_hook<
        boost::intrusive::link_mode< boost::intrusive::auto_unlink >
    > {
    public:
        explicit DatasetImpl(LocalDatasetManager* manager);

        DatasetImpl(const DatasetImpl& impl);

        void Initialize(uint64_t sequence, DatasetImpl* father);
        virtual void AddReference();
        virtual void Release();
        virtual DatasetImpl* GetChild(const toft::StringPiece& key);

        virtual bool IsReady();

        virtual void Commit();
        virtual Iterator* Discard();
        virtual Iterator* NewIterator();

        virtual util::Arena* AcquireArena();
        virtual void ReleaseArena();
        virtual void Emit(const toft::StringPiece& value);

        void Dump();

    private:
        struct MemoryRecord : public boost::intrusive::slist_base_hook<
            boost::intrusive::link_mode< boost::intrusive::normal_link >
        > {
            toft::StringPiece data;
        };

        typedef boost::intrusive::slist<
            MemoryRecord,
            boost::intrusive::cache_last<true>,  // for enabling push_back
            boost::intrusive::constant_time_size<true>
        > MemoryChunk;

        class MemoryChunkIterator;
        class DiskChunkIterator;

        leveldb::Slice AsKey(uint64_t offset);

        leveldb::Slice AsValue(const toft::StringPiece& value) {
            return leveldb::Slice(value.data(), value.size());
        }

        void Append(MemoryChunk* chunk);
        void Append(const leveldb::Slice& anchor, uint64_t size);

        LocalDatasetManager* m_manager;
        DatasetImpl* m_father;
        int m_ref;
        SharedArena m_temporary_arena;

        enum {
            RELEASED,
            NORMAL,
            READY,
            DISCARDED
        } m_state;
        uint64_t m_record_count;

        // all memory related data structures are allocated from memory arena
        SharedArena m_memory_arena;
        MemoryChunk m_memory_chunk;
        MemoryRecord* m_first_memory_record;
        MemoryRecord* m_last_memory_record;

        // key[0] is fixed for a Dataset, key[1] is for different records
        uint64_t m_dumping_key[2];
        // in case of dumpping data into disks, we do not care the overhead of
        // allocating objects from heap
        boost::ptr_vector<DiskChunkIterator> m_disk_iterators;
    };

    LocalDatasetManager();
    ~LocalDatasetManager();

    void Initialize(const std::string& swap_dir, uint64_t max_used_bytes);

    virtual DatasetImpl* GetDataset(const std::string& identity);

private:
    typedef boost::intrusive::list<
        DatasetImpl,
        boost::intrusive::constant_time_size<false>  // for enabling unlink
    > DatasetList;

    DatasetImpl* AllocateDataset(DatasetImpl* father);

    void CheckMemoryUsage();

    uint64_t m_max_used_bytes;
    SharedArena m_arena;
    boost::scoped_ptr<leveldb::DB> m_db;

    DatasetList m_memory_datasets;  // save records in current arena
    DatasetList m_disk_datasets;  // save records in disk
    DatasetList m_final_datasets;  // committed or discarded
    DatasetList m_released_datasets;  // released

#if __GNUC__ == 3
    std::deque<DatasetImpl> m_dataset_pool;
#else
    boost::container::deque<DatasetImpl> m_dataset_pool;  // use boost deque for emplace
#endif // __GNUC__ == 3

    uint64_t m_next_sequence;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_LOCAL_DATASET_H_
