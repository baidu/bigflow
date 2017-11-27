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

#include "flume/runtime/common/executor_impl.h"

#include "boost/foreach.hpp"

#include "flume/runtime/common/general_dispatcher.h"
#include "flume/runtime/common/single_dispatcher.h"
#include "flume/runtime/dataset.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace internal {

class ExecutorInputManager::SourceImpl : public Source {
public:
    explicit SourceImpl(ExecutorInputManager* base) : _base(base), _source(NULL) {}

    void setup(Source* source) {
        _source = source;
    }

protected:
    virtual Handle* RequireStream(uint32_t flag, StreamCallback* callback, DoneCallback* done) {
        int index = _base->_active_inputs.size();
        _base->_active_inputs.push_back(false);
        if (done == NULL) {
            return _source->RequireStream(
                flag, callback,
                toft::NewPermanentClosure(_base, &ExecutorInputManager::on_input_done, index)
            );
        } else {
            _base->_delegate_dones.push_back(done);
            return _source->RequireStream(
                flag, callback,
                toft::NewPermanentClosure(
                        _base, &ExecutorInputManager::on_input_done_with_callback, index, done
                )
            );
        }
    }

    virtual Handle* RequireIterator(IteratorCallback* iterator_callback, DoneCallback* done) {
        int index = _base->_active_inputs.size();
        _base->_active_inputs.push_back(false);
        if (done == NULL) {
            return _source->RequireIterator(
                iterator_callback,
                toft::NewPermanentClosure(_base, &ExecutorInputManager::on_input_done, index)
            );
        } else {
            _base->_delegate_dones.push_back(done);
            return _source->RequireIterator(
                iterator_callback,
                toft::NewPermanentClosure(
                        _base, &ExecutorInputManager::on_input_done_with_callback, index, done
                )
            );
        }
    }

private:
    ExecutorInputManager* _base;
    Source* _source;
};

ExecutorInputManager::ExecutorInputManager(const PbExecutor& message,
                                           uint32_t input_scope_level) :
        _input_scope_level(input_scope_level) {
    for (int i = 0; i < message.input_size(); ++i) {
        std::auto_ptr<SourceImpl> source(new SourceImpl(this));
        _inputs.insert(message.input(i), source);
    }
}

ExecutorInputManager::~ExecutorInputManager() {}

void ExecutorInputManager::setup(const std::map<std::string, Source*>& sources) {
    typedef boost::ptr_map<std::string, SourceImpl>::iterator Iterator;
    for (Iterator ptr = _inputs.begin(); ptr != _inputs.end(); ++ptr) {
        CHECK_EQ(1, sources.count(ptr->first));
        ptr->second->setup(sources.find(ptr->first)->second);
    }
}

Source* ExecutorInputManager::get(const std::string& identity) {
    CHECK_NE(_inputs.count(identity), 0);
    return _inputs.find(identity)->second;
}

void ExecutorInputManager::on_input_done(int index) {
    _active_inputs[index] = false;
    if (_active_inputs.none()) {
        on_input_done();
        _keys.pop_back();
    }
}

void ExecutorInputManager::on_input_done_with_callback(int index,
                                                       toft::Closure<void ()>* callback) {
    callback->Run();
    _active_inputs[index] = false;
    if (_active_inputs.none()) {
        on_input_done();
        _keys.pop_back();
    }
}

DispatcherManager::DispatcherManager(const PbExecutor& message,
                                     DatasetManager* dataset_manager) : _last_priority(0) {
    std::vector<uint32_t> priority_counts;
    for (int i = 0; i < message.dispatcher_size(); ++i) {
        const PbExecutor::Dispatcher& config = message.dispatcher(i);
        if (config.has_priority()) {
            uint32_t priority = config.priority();
            if (priority >= priority_counts.size()) {
                priority_counts.resize(priority + 1, 0);
            }
            ++priority_counts[priority];
        }
    }

    _prior_dispatcher_counts.resize(priority_counts.size() + 1, 0);
    for (size_t i = 1; i < _prior_dispatcher_counts.size(); ++i) {
        _prior_dispatcher_counts[i] = _prior_dispatcher_counts[i - 1] + priority_counts[i - 1];
    }

    _dispatchers.resize(message.dispatcher_size());
    std::vector<uint32_t> next_indexes = _prior_dispatcher_counts;
    for (int i = 0; i < message.dispatcher_size(); ++i) {
        const PbExecutor::Dispatcher& config = message.dispatcher(i);
        uint32_t priority = config.priority();

        std::auto_ptr<Dispatcher> dispatcher;
        if (config.usage_count() != 1) {
            dispatcher.reset(
                    new GeneralDispatcher(config.identity(), message.scope_level())
            );
        } else if (config.need_dataset()) {
            dispatcher.reset(
                    new SingleDispatcher(config.identity(), message.scope_level())
            );
        } else {
            dispatcher.reset(
                    new SingleStreamDispatcher(config.identity(), message.scope_level())
            );
        }

        if (config.need_dataset()) {
            dispatcher->SetDatasetManager(dataset_manager);
        }

        if (config.has_objector()) {
            dispatcher->SetObjector(config.objector());
        }

        uint32_t index = -1;
        if (config.has_priority()) {
            index = next_indexes[config.priority()]++;
        } else {
            index = next_indexes.back()++;
        }
        _dispatchers[index] = dispatcher.get();

        _dispatcher_table.insert(config.identity(), dispatcher);
        _priority_table.insert(std::make_pair(config.identity(), priority));
    }
}

DispatcherManager::~DispatcherManager() {}

Dispatcher* DispatcherManager::get(const std::string& identity) {
    DispatcherTable::iterator ptr = _dispatcher_table.find(identity);
    if (ptr == _dispatcher_table.end()) {
        return NULL;
    }
    return ptr->second;
}

PriorityDispatcher DispatcherManager::get_priority_dispatcher(const std::string& identity) {
    Dispatcher* dispatcher = get(identity);
    PriorityTable::iterator ptr = _priority_table.find(identity);
    if (ptr == _priority_table.end()) {
        return PriorityDispatcher();
    }
    return PriorityDispatcher(dispatcher, ptr->second);
}

Dispatcher* DispatcherManager::replace(const std::string& identity, Dispatcher* dispatcher) {
    DispatcherTable::auto_type origin =
            _dispatcher_table.replace(_dispatcher_table.find(identity), dispatcher);
    CHECK_NOTNULL(origin.get());

    for (size_t i = 0; i < _dispatchers.size(); ++i) {
        if (_dispatchers[i] == origin.get()) {
            _dispatchers[i] = dispatcher;
        }
    }

    return origin.release();
}

void DispatcherManager::begin_group(const toft::StringPiece& key,
                                    uint32_t target_scope_level) {
    for (size_t i = 0; i < _dispatchers.size(); ++i) {
        Dispatcher* dispatcher = _dispatchers[i];
        if (dispatcher->GetScopeLevel() != target_scope_level) {
            // BeginGroup may have been called by ExecutorRunner already
            dispatcher->BeginGroup(key);
        }
        DCHECK_EQ(target_scope_level, dispatcher->GetScopeLevel());
    }
}

void DispatcherManager::finish_group() {
    for (size_t i = 0; i < _dispatchers.size(); ++i) {
        _dispatchers[i]->FinishGroup();
    }
}

void DispatcherManager::close_prior_dispatchers(uint32_t priority) {
    DCHECK_GE(priority, _last_priority);
    DCHECK_LT(priority, _prior_dispatcher_counts.size());

    if (_last_priority < priority) {
        uint32_t begin = _prior_dispatcher_counts[_last_priority];
        uint32_t end = _prior_dispatcher_counts[priority];
        for (uint32_t i = begin; i < end; ++i) {
            _dispatchers[i]->Done();
        }
        _last_priority = priority;
    }
}

void DispatcherManager::done() {
    for (size_t i = _prior_dispatcher_counts[_last_priority]; i < _dispatchers.size(); ++i) {
        _dispatchers[i]->Done();
    }
    _last_priority = 0;
}

}  // namespace internal
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
