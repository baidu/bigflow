/***************************************************************************
 *
 * Copyright (c) 2013 Baidu, Inc. All Rights Reserved.
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
// Interfaces for communicating records between executors

#ifndef FLUME_RUNTIME_DISPATCHER_H_
#define FLUME_RUNTIME_DISPATCHER_H_

#include <vector>

#include "toft/base/closure.h"

#include "flume/core/iterator.h"

namespace baidu {
namespace flume {
namespace runtime {

class Dataset;
class DatasetManager;

// Source is designed according to Observer Pattern. Datas are dispatched to listeners in
// a per-group manner.
class Source {
public:
    typedef toft::Closure<void ()> DoneCallback;
    typedef toft::Closure<void (const std::vector<toft::StringPiece>&,
                                void*, const toft::StringPiece&)> StreamCallback;  // NOLINT
    typedef toft::Closure<void (core::Iterator*)> IteratorCallback;

    enum RequireFlag {
        REQUIRE_KEY = 1, // 1 << 0
        REQUIRE_OBJECT = 2, // 1 << 1
        REQUIRE_BINARY = 4  // 1 << 2
    };

    // Handle represents a listener who need datas.
    class Handle {
    public:
        virtual ~Handle() {}

        // Should not be called after eof or after an iterator are given.
        virtual void Done() = 0;
    };

public:
    virtual ~Source() {}

    // Create a listener handle, need iterable objects.
    // In the processing of one date group, iterator_callback is called when all datas of this
    // group are ready.
    // If the case of Fallback, fallback_callback is used to dispatch object stream, and
    // iterator_callback will not be called any more.
    // done is always called, even if an iterator is delivered or Handle::Done is called
    virtual Handle* RequireIterator(IteratorCallback* iterator_callback, DoneCallback* done) = 0;

    // Create a listener handle, need raw objects.
    // In the processing of one data group, listener will be called when an record is
    // generated from this Source, done is called when the whole group is end.
    virtual Handle* RequireStream(uint32_t flag,
                                  StreamCallback* callback, DoneCallback* done) = 0;
};

// Dispatcher is the sender side of Source
class Dispatcher {
public:
    virtual ~Dispatcher() {}

    virtual void SetDatasetManager(DatasetManager* dataset_manager) = 0;

    virtual void SetObjector(const PbEntity& message) = 0;

    // Get the source for particular scope
    virtual Source* GetSource(uint32_t scope_level) = 0;

    // Same meaning as executor
    virtual void BeginGroup(const toft::StringPiece& key) = 0;
    virtual void BeginGroup(const toft::StringPiece& key, Dataset* dataset) = 0;
    virtual void FinishGroup() = 0;

    // return whether more data is needed
    virtual uint32_t GetScopeLevel() = 0;
    virtual Dataset* GetDataset(uint32_t scope_level) = 0;
    virtual bool IsAcceptMore() = 0;

    // Dispatch data to downstreams
    virtual bool EmitObject(void* object) = 0;
    virtual bool EmitObject(const std::vector<toft::StringPiece>& keys, void* object) = 0;
    virtual bool EmitBinary(const toft::StringPiece& binary) = 0;
    virtual bool EmitBinary(const std::vector<toft::StringPiece>& keys,
                            const toft::StringPiece& binary) = 0;

    virtual void Done() = 0;
};

struct PriorityDispatcher {
    Dispatcher* dispatcher;
    uint32_t priority;

    PriorityDispatcher(): dispatcher(NULL), priority(0) {}

    PriorityDispatcher(Dispatcher* dispatcher_, uint32_t priority_):
        dispatcher(dispatcher_), priority(priority_) {}
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_DISPATCHER_H_
