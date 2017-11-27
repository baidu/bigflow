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
//

#ifndef FLUME_RUNTIME_COMMON_PARTIAL_EXECUTOR_H_
#define FLUME_RUNTIME_COMMON_PARTIAL_EXECUTOR_H_

#include <map>
#include <string>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/ptr_container/ptr_map.hpp"
#include "boost/scoped_array.hpp"

#include "flume/core/objector.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/entity_dag.h"
#include "flume/runtime/common/executor_impl.h"
#include "flume/runtime/common/general_dispatcher.h"
#include "flume/runtime/common/sub_executor_manager.h"
#include "flume/runtime/dataset.h"
#include "flume/runtime/executor.h"
#include "flume/util/bitset.h"

namespace baidu {
namespace flume {
namespace runtime {

class PartialExecutor : public Executor {
public:
    PartialExecutor();  // default 128M buffer
    explicit PartialExecutor(int buffer_metabytes);

    void Initialize(const PbExecutor& message, const std::vector<Executor*>& childs,
                    uint32_t input_scope_level, DatasetManager* dataset_manager);

    virtual void Setup(const std::map<std::string, Source*>& sources);

    virtual Source* GetSource(const std::string& id, unsigned scope_level);

    virtual void BeginGroup(const toft::StringPiece& key);

    virtual void FinishGroup();

private:
    struct Record;
    struct Key;

    struct Output {
        uint32_t index;
        bool need_hash;
        std::vector<uint32_t> priorities;

        Dispatcher* dispatcher;
        core::Objector* objector;
    };

    static const int kDefaultBufferSize = 128 * 1024 * 1024;  // 128M

    static bool CompareRecord(const Record*, const Record*);

    void OnInputCome(int input, const std::vector<toft::StringPiece>& keys,
                     void* object, const toft::StringPiece& binary);
    void OnInputDone(int input);
    void OnSortedOutput(Output* output, const std::vector<toft::StringPiece>& keys, void* object);
    void OnDirectOutput(Output* output, const std::vector<toft::StringPiece>& keys, void* object);

    template<typename T> T* New();  // new from buffer

    void ResetBuffer();
    void FlushBuffer();
    bool PushBack(Output* output, const std::vector<toft::StringPiece>& keys, void* object);

    const size_t m_buffer_size;
    toft::scoped_array<char> m_buffer;

    PbExecutor m_message;
    SubExecutorManager m_childs;
    boost::scoped_ptr<internal::DispatcherManager> m_dispatchers;

    boost::ptr_vector<core::Objector> m_objectors;
    boost::ptr_vector<Output> m_outputs;

    EntityDag m_dag;

    std::vector<toft::StringPiece> m_keys;
    util::Bitset m_active_bitset;
    EntityDag::Instance* m_instance;

    char* m_buffer_ptr;
    char* m_buffer_end;
    Record** m_records;
    size_t m_record_size;

    boost::ptr_map<std::string, Source> m_partial_source;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_COMMON_PARTIAL_EXECUTOR_H_
