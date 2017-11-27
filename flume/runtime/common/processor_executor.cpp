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
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#include "flume/runtime/common/processor_executor.h"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/processor.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

using core::Entity;
using core::Objector;
using core::Processor;

namespace internal {

ProcessorRunner::ProcessorRunner() :
        _context(NULL), _output(NULL), _in_processing(false), _processor(NULL) {}

ProcessorRunner::~ProcessorRunner() {}

void ProcessorRunner::setup(const PbExecutor& message, ExecutorContext* context) {
    CHECK_EQ(message.type(), PbExecutor::PROCESSOR);
    _message = message.processor_executor();
    set_message(_message);

    _context = context;
    _output = _context->output(_message.identity());

    _initial_inputs.resize(_message.source_size(), NULL);
    _prepared_inputs.resize(_message.source_size(), NULL);
    for (int i = 0; i < _message.source_size(); ++i) {
        const PbProcessorExecutor::Source& source = _message.source(i);
        Source::Handle* handle = NULL;
        switch (source.type()) {
            case PbProcessorExecutor::DUMMY: {
                if (source.is_prepared()) {
                    std::auto_ptr<DummyIterator> iterator(new DummyIterator());
                    _initial_inputs[i] = iterator.get();
                    _dummy_iterators.push_back(iterator);
                }
                break;
            }
            case PbProcessorExecutor::REQUIRE_STREAM: {
                handle = _context->input(source.identity())->RequireStream(
                    Source::REQUIRE_OBJECT,
                    toft::NewPermanentClosure(this, &ProcessorRunner::on_input_come, i),
                    NULL
                );
                break;
            }
            default: {
                std::auto_ptr<Source::IteratorCallback> callback;
                if (source.is_prepared()) {
                    callback.reset(toft::NewPermanentClosure(
                            this, &ProcessorRunner::on_prepared_iterator_come, i
                    ));
                } else {
                    callback.reset(toft::NewPermanentClosure(
                            this, &ProcessorRunner::on_flushing_iterator_come, i
                    ));
                }
                handle = _context->input(source.identity())->RequireIterator(
                    callback.release(), NULL
                );
            }
        }

        _handles.push_back(handle);
    }
}

void ProcessorRunner::begin_group(const std::vector<toft::StringPiece>& keys) {
    _keys = keys;
    _in_processing = false;
    _is_done = false;
    _prepared_inputs = _initial_inputs;
}

void ProcessorRunner::on_prepared_iterator_come(int index, core::Iterator* iterator) {
    DCHECK(!_in_processing);
    _prepared_inputs[index] = iterator;
}

void ProcessorRunner::on_flushing_iterator_come(int index, core::Iterator* iterator) {
    if (_is_done) {
        iterator->Done();
    } else if (_in_processing) {
        flush_iterator(index, iterator);
    } else {
        _pending_inputs.push_back(std::make_pair(index, iterator));
    }
}

void ProcessorRunner::on_input_come(int index, const std::vector<toft::StringPiece>& keys,
                                    void* object, const toft::StringPiece& binary) {
    check_is_processing();
    if (_in_processing) {
        _processor->Process(index, object);
    }
}

inline void ProcessorRunner::check_is_processing() {
    if (!_in_processing && !_is_done) {
        // delay processing until first stream record or end_group
        _in_processing = true;

        _processor = begin_processing(_keys, _prepared_inputs, this);
        if (_processor == NULL) {
            Done();  // Emitter::Done
        }

        for (size_t i = 0; i < _pending_inputs.size(); ++i) {
            flush_iterator(_pending_inputs[i].first, _pending_inputs[i].second);
        }
        _pending_inputs.clear();
    }
}

inline void ProcessorRunner::flush_iterator(int index, core::Iterator* iterator) {
    while (_in_processing && iterator->HasNext()) {
        _processor->Process(index, iterator->NextValue());
    }
    iterator->Done();
}

void ProcessorRunner::end_group() {
    check_is_processing();

    _is_done = true;
    end_processing(_processor, !_in_processing);  // only Emitter::Done reset _in_processing
    _processor = NULL;
    _in_processing = false;

    for (size_t i = 0; i < _prepared_inputs.size(); ++i) {
        if (_prepared_inputs[i] != NULL) {
            _prepared_inputs[i]->Done();
        }
    }
    _prepared_inputs.clear();
}

bool ProcessorRunner::Emit(void* object) {
    if (!_in_processing) {
        return false;
    }
    return _output->EmitObject(object);
}

void ProcessorRunner::Done() {
    if (!_is_done) {
        for (size_t i = 0; i < _handles.size(); ++i) {
            if (_handles[i] != NULL) {
                _handles[i]->Done();
            }
        }
        _output->Done();
    }
    _is_done = true;

    _in_processing = false;
}

}  // namespace internal

void NormalProcessorRunner::set_message(const PbProcessorExecutor& message) {
    _processor.reset(Entity<Processor>::From(message.processor()).CreateAndSetup());
    if (message.has_partial_key_number()) {
        _key_number = message.partial_key_number();
    } else {
        _key_number = 0;
    }
}

core::Processor* NormalProcessorRunner::begin_processing(
        const std::vector<toft::StringPiece>& keys,
        const std::vector<core::Iterator*>& inputs,
        core::Emitter* emitter) {
    if (_key_number == 0) {
        _processor->BeginGroup(keys, inputs, emitter);
    } else {
        _partial_keys.assign(keys.begin(), keys.begin() + _key_number);
        _processor->BeginGroup(_partial_keys, inputs, emitter);
    }
    return _processor.get();
}

void NormalProcessorRunner::end_processing(core::Processor* processor, bool is_done) {
    CHECK_EQ(processor, _processor.get());
    _processor->EndGroup();
}

void StatusProcessorRunner::SetStatusTable(StatusTable* status_table) {
    _status_table = status_table;
}

void StatusProcessorRunner::set_message(const PbProcessorExecutor& message) {
    _message = message;
}

core::Processor* StatusProcessorRunner::begin_processing(
        const std::vector<toft::StringPiece>& keys,
        const std::vector<core::Iterator*>& inputs,
        core::Emitter* emitter) {
    _visitor = _status_table->GetNodeVisitor(_message.identity(), keys);
    CHECK_NOTNULL(_visitor);
    if (!_visitor->IsValid()) {
        _visitor->Release();
        _visitor = NULL;
        return NULL;
    }

    Entity<Processor> entity = Entity<Processor>::From(_message.processor());
    _visitor->Read(entity.mutable_config());

    std::auto_ptr<Processor> processor(entity.CreateAndSetup());
    processor->BeginGroup(keys, inputs, emitter);
    return processor.release();
}

void UpdateStatusProcessorRunner::end_processing(core::Processor* processor, bool is_done) {
    std::auto_ptr<core::Processor> deleter(processor);
    if (processor == NULL) {
        return;
    }

    if (is_done) {
        _visitor->Invalidate();
        processor->EndGroup();
    } else {
        _visitor->Update(processor->Synchronize());
    }

    _visitor->Release();
    _visitor = NULL;
}

void FlushStatusProcessorRunner::end_processing(core::Processor* processor, bool is_done) {
    std::auto_ptr<core::Processor> deleter(processor);
    if (processor == NULL) {
        return;
    }

    _visitor->Invalidate();
    processor->EndGroup();

    _visitor->Release();
    _visitor = NULL;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
