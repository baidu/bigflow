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
// A simple implementation of Dispatcher, dispatch datas to only one user.

#ifndef FLUME_RUNTIME_COMMON_SINGLE_DISPATCHER_H_
#define FLUME_RUNTIME_COMMON_SINGLE_DISPATCHER_H_

#include <string>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/function.hpp"
#include "boost/scoped_array.hpp"
#include "boost/scoped_ptr.hpp"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/iterator.h"
#include "flume/core/objector.h"
#include "flume/proto/entity.pb.h"
#include "flume/runtime/dataset.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/util/serialize_buffer.h"

namespace baidu {
namespace flume {
namespace runtime {

class SingleDispatcher : public Dispatcher {
public:
    SingleDispatcher(const std::string& identity, uint32_t scope_level);
    virtual ~SingleDispatcher();

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
    typedef Source::DoneCallback DoneCallback;
    typedef Source::StreamCallback StreamCallback;
    typedef Source::IteratorCallback IteratorCallback;

    class SourceImpl;
    class HandleImpl;
    class IteratorHandleImpl;
    class IteratorImpl;

private:
    void FlushDataset(const std::vector<toft::StringPiece>& keys, Dataset* dataset);
    bool SendObject(const std::vector<toft::StringPiece>& keys, void* object);
    bool SendBinary(const std::vector<toft::StringPiece>& keys, const toft::StringPiece& binary);

    void ReleaseRetiredDataset(Dataset* dataset = NULL);
    void LeaveLastLevel();

    uint32_t current_level() {
        return m_keys.size() - 1;
    }

    bool is_require_object() {
        return m_dispatch_flag & Source::REQUIRE_OBJECT;
    }

    bool is_require_binary() {
        return m_dispatch_flag & Source::REQUIRE_BINARY;
    }

private:
    DatasetManager* m_dataset_manager;
    boost::scoped_ptr<core::Objector> m_objector;
    SerializeBuffer m_buffer;

    std::string m_identity;
    uint32_t m_input_level;
    boost::ptr_vector<SourceImpl> m_sources;
    boost::scoped_ptr<HandleImpl> m_handle;

    uint32_t m_dispatch_level;
    uint32_t m_dispatch_flag;
    boost::scoped_ptr<IteratorImpl> m_iterator;

    boost::scoped_ptr<StreamCallback> m_stream_callback;
    boost::scoped_ptr<DoneCallback> m_done_callback;
    boost::scoped_ptr<IteratorCallback> m_iterator_callback;

    std::vector<toft::StringPiece> m_keys;
    std::vector<Dataset*> m_datasets;

    bool m_need_more;
    Dataset* m_retired_dataset;
};

class SingleStreamDispatcher : public Dispatcher {
public:
    SingleStreamDispatcher(const std::string& identity, uint32_t scope_level);
    virtual ~SingleStreamDispatcher();

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
    typedef Source::DoneCallback DoneCallback;
    typedef Source::StreamCallback StreamCallback;
    typedef Source::IteratorCallback IteratorCallback;

    class SourceImpl;
    class HandleImpl;

private:
    void FlushDataset(const std::vector<toft::StringPiece>& keys, Dataset* dataset);
    bool SendObject(const std::vector<toft::StringPiece>& keys, void* object);
    bool SendBinary(const std::vector<toft::StringPiece>& keys, const toft::StringPiece& binary);

    void ReleaseRetiredDataset(Dataset* dataset = NULL);
    void LeaveLastLevel();

    uint32_t current_level() {
        return m_keys.size() - 1;
    }

    bool is_require_object() {
        return m_dispatch_flag & Source::REQUIRE_OBJECT;
    }

    bool is_require_binary() {
        return m_dispatch_flag & Source::REQUIRE_BINARY;
    }

private:
    boost::scoped_ptr<core::Objector> m_objector;
    SerializeBuffer m_buffer;

    std::string m_identity;
    uint32_t m_input_level;
    boost::ptr_vector<SourceImpl> m_sources;
    boost::scoped_ptr<HandleImpl> m_handle;

    uint32_t m_dispatch_level;
    uint32_t m_dispatch_flag;

    boost::scoped_ptr<StreamCallback> m_stream_callback;
    boost::scoped_ptr<DoneCallback> m_done_callback;
    boost::scoped_ptr<IteratorCallback> m_iterator_callback;

    std::vector<toft::StringPiece> m_keys;

    bool m_need_more;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_SINGLE_DISPATCHER_H_
