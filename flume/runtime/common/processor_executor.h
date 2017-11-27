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

#ifndef FLUME_RUNTIME_COMMON_PROCESSOR_EXECUTOR_H_
#define FLUME_RUNTIME_COMMON_PROCESSOR_EXECUTOR_H_

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/scoped_ptr.hpp"
#include "toft/base/closure.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/core/processor.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/executor_base.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/status_table.h"
#include "flume/util/bitset.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace internal {

class ProcessorRunner : public ExecutorRunner, public core::Emitter {
public:
    ProcessorRunner();
    virtual ~ProcessorRunner();

    virtual void setup(const PbExecutor& message, ExecutorContext* context);

    // called when all inputs is ready
    virtual void begin_group(const std::vector<toft::StringPiece>& keys);

    // called after all inputs is done.
    virtual void end_group();

protected:
    virtual bool Emit(void *object);

    virtual void Done();

private:
    class DummyIterator : public core::Iterator {
    public:
        virtual bool HasNext() const { return false; }
        virtual void* NextValue() {
            LOG(FATAL) << "Dummy iterator contains no records.";
            return NULL;
        }
        virtual void Reset() {}
        virtual void Done() {}
    };

    virtual void set_message(const PbProcessorExecutor& message) = 0;

    virtual core::Processor* begin_processing(const std::vector<toft::StringPiece>& keys,
                                              const std::vector<core::Iterator*>& inputs,
                                              core::Emitter* emitter) = 0;

    virtual void end_processing(core::Processor* processor, bool is_done) = 0;

    void on_prepared_iterator_come(int index, core::Iterator* iterator);
    void on_flushing_iterator_come(int index, core::Iterator* iterator);
    void on_input_come(int index, const std::vector<toft::StringPiece>& keys,
                       void* object, const toft::StringPiece& binary);

    void check_is_processing();
    void flush_iterator(int index, core::Iterator* iterator);

    PbProcessorExecutor _message;
    boost::ptr_vector<DummyIterator> _dummy_iterators;

    ExecutorContext* _context;
    Dispatcher* _output;
    std::vector<Source::Handle*> _handles;
    std::vector<core::Iterator*> _initial_inputs;

    std::vector<toft::StringPiece> _keys;
    bool _in_processing;
    bool _is_done;
    core::Processor* _processor;

    std::vector<core::Iterator*> _prepared_inputs;
    std::vector< std::pair<size_t, core::Iterator*> > _pending_inputs;
};

}  // namespace internal

class NormalProcessorRunner : public internal::ProcessorRunner {
public:
    NormalProcessorRunner() : _key_number(0), _processor(NULL) {}
    ~NormalProcessorRunner() {}

    virtual void set_message(const PbProcessorExecutor& message);

    virtual core::Processor* begin_processing(const std::vector<toft::StringPiece>& keys,
                                              const std::vector<core::Iterator*>& inputs,
                                              core::Emitter* emitter);

    virtual void end_processing(core::Processor* processor, bool is_done);

private:
    uint32_t _key_number;
    toft::scoped_ptr<core::Processor> _processor;
    std::vector<toft::StringPiece> _partial_keys;
};

class StatusProcessorRunner :  public internal::ProcessorRunner {
public:
    StatusProcessorRunner() : _status_table(NULL), _visitor(NULL) {}
    ~StatusProcessorRunner() {}

    virtual void set_message(const PbProcessorExecutor& message);

    void SetStatusTable(StatusTable* status_table);

    virtual core::Processor* begin_processing(const std::vector<toft::StringPiece>& keys,
                                              const std::vector<core::Iterator*>& inputs,
                                              core::Emitter* emitter);

protected:
    PbProcessorExecutor _message;
    StatusTable* _status_table;

    StatusTable::NodeVisitor* _visitor;
};

class UpdateStatusProcessorRunner : public StatusProcessorRunner {
public:
    virtual void end_processing(core::Processor* processor, bool is_done);
};


class FlushStatusProcessorRunner : public StatusProcessorRunner {
public:
    virtual void end_processing(core::Processor* processor, bool is_done);
};

Executor* new_shuffle_executor(uint32_t partition,
                               uint32_t input_scope_level, const PbExecutor& message,
                               const std::vector<Executor*>& childs,
                               DatasetManager* dataset_manager);

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_PROCESSOR_EXECUTOR_H_
