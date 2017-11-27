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
//
// A general implementation of Dispatcher

#ifndef FLUME_RUNTIME_COMMON_GENERAL_DISPATCHER_H_
#define FLUME_RUNTIME_COMMON_GENERAL_DISPATCHER_H_

#include <string>
#include <vector>

#include "boost/dynamic_bitset.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/scoped_array.hpp"
#include "boost/scoped_ptr.hpp"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/proto/entity.pb.h"
#include "flume/runtime/dataset.h"
#include "flume/runtime/dispatcher.h"

namespace baidu {
namespace flume {
namespace runtime {

class GeneralDispatcher : public Dispatcher {
public:
    GeneralDispatcher(const std::string& identity, uint32_t scope_level);
    virtual ~GeneralDispatcher();

    virtual void SetDatasetManager(DatasetManager* dataset_manager);

    virtual void SetObjector(const PbEntity& message);

    // Get the source for particular scope
    virtual Source* GetSource(uint32_t scope_level);

    // Same meaning as executor
    virtual void BeginGroup(const toft::StringPiece& key);
    virtual void BeginGroup(const toft::StringPiece& key, Dataset* dataset);
    virtual void FinishGroup();

    // return whether more data is needed
    virtual uint32_t GetScopeLevel();
    virtual Dataset* GetDataset(uint32_t scope_level);
    virtual bool IsAcceptMore();

    // Dispatch data to downstreams
    virtual bool EmitObject(void* object);
    virtual bool EmitObject(const std::vector<toft::StringPiece>& keys, void* object);
    virtual bool EmitBinary(const toft::StringPiece& binary);
    virtual bool EmitBinary(const std::vector<toft::StringPiece>& keys,
                            const toft::StringPiece& binary);
    virtual void Done();

private:
    typedef boost::dynamic_bitset<unsigned long> Bitset;
    typedef Source::DoneCallback DoneCallback;
    typedef Source::StreamCallback StreamCallback;
    typedef Source::IteratorCallback IteratorCallback;

    struct ScopeLevelConfig {
        Bitset zero_mask;        // zero_mask.size() == total_handle_size
        Bitset set_mask;         // used to set bit for handles of this level
        Bitset clear_mask;       // used to clear bit for handles of this level

        Bitset object_bitset;     // set if handle requires object
        Bitset binary_bitset;     // set if handle requires binary
        Bitset iterator_bitset;   // set if handle requires iterator
    };

    struct ScopeLevelRuntime {
        Bitset iterator_bitset;   // set if dataset should be dispatched as iterator
        Bitset done_bitset;       // set if Handle::Done is called

        Dataset* dataset;         // records cache
    };

    class SourceImpl;
    class HandleImpl;
    class IteratorHandleImpl;
    class IteratorImpl;

private:
    bool SendObject(const std::vector<toft::StringPiece>& keys, void* object);
    bool SendBinary(const std::vector<toft::StringPiece>& keys, const toft::StringPiece& binary);

    uint32_t AllocateHandle(uint32_t scope_level);

    void ReleaseLastDataset();
    void RetireLastRuntime();

    void DispatchDone(const Bitset& bitset);

    void DispatchIterator(const Bitset& bitset, Dataset* dataset);

    void DispatchStream(const Bitset& bitset,
                        const std::vector<toft::StringPiece>& keys,
                        void* object, const toft::StringPiece& binary);

    void FlushDataset(const Bitset& dispatch_bitset, const Bitset& object_bitset,
                      const std::vector<toft::StringPiece>& keys, Dataset::Iterator* iterator);

    bool CheckForLastScope();
    bool HasDownstreams();
    void PerformEarlyFallback();

private:
    DatasetManager* m_dataset_manager;
    boost::scoped_ptr<core::Objector> m_objector;

    std::string m_identity;
    boost::ptr_vector<Source> m_sources;
    std::vector<ScopeLevelConfig> m_configs;

    std::vector<toft::StringPiece> m_keys;
    std::vector<ScopeLevelRuntime> m_runtimes;

    uint32_t m_handle_count;
    std::vector<StreamCallback*> m_stream_callbacks;
    std::vector<DoneCallback*> m_done_callbacks;
    std::vector<IteratorCallback*> m_iterator_callbacks;
    boost::ptr_vector<IteratorImpl> m_iterators;

    Bitset m_stream_bitset;    // indicate handles to dispatch stream
    Bitset m_object_bitset;    // indicate handles to dispatch object
    Bitset m_binary_bitset;    // indicate handles to dispatch binary
    Bitset m_cache_bitset;     // indicate handles to cache datas

    // we keep dataset for last retired runtime until next scope changing.
    Dataset* m_last_dataset;

    // 32k is enough. For bigger size, malloc is not bottleneck
    static const size_t kBufferSize = 32 * 1024;
    boost::scoped_array<char> m_buffer;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_GENERAL_DISPATCHER_H_
