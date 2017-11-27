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

#ifndef FLUME_RUNTIME_COMMON_EXECUTOR_IMPL_H_
#define FLUME_RUNTIME_COMMON_EXECUTOR_IMPL_H_

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "boost/ptr_container/ptr_map.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/scoped_ptr.hpp"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/sub_executor_manager.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/executor.h"
#include "flume/util/bitset.h"

namespace baidu {
namespace flume {
namespace runtime {

class DatasetManager;

namespace internal {

class ExecutorInputManager {
public:
    explicit ExecutorInputManager(const PbExecutor& message, uint32_t input_scope_level);
    virtual ~ExecutorInputManager();

    Source* get(const std::string& identity);

    void setup(const std::map<std::string, Source*>& sources);

    uint32_t input_scope_level() const { return _input_scope_level; }

    uint32_t current_scope_level() { return _keys.size() - 1; }

    void begin_group(const toft::StringPiece& key) {
        _keys.push_back(key);
        if (current_scope_level() == _input_scope_level) {
            _active_inputs.set();
        }
    }

    void finish_group() {
        if (current_scope_level() == _input_scope_level) {
            on_input_ready();
            if (_active_inputs.none()) {
                on_input_done();
                _keys.pop_back();
            }
        } else {
            _keys.pop_back();
        }
    }

protected:
    virtual void on_input_ready() = 0;
    virtual void on_input_done() = 0;

protected:
    class SourceImpl;
    class DispatcherImpl;
    typedef util::Bitset Bitset;

    void on_input_done(int index);
    void on_input_done_with_callback(int index, toft::Closure<void ()>* callback);

    boost::ptr_map<std::string, SourceImpl> _inputs;
    boost::ptr_vector<toft::ClosureBase> _delegate_dones;

    const uint32_t _input_scope_level;
    std::vector<toft::StringPiece> _keys;
    Bitset _active_inputs;
};

template<typename ExecutorType, bool kHasInput> class ExecutorInputImpl;

template<typename ExecutorType>
class ExecutorInputImpl<ExecutorType, true> : public ExecutorInputManager {
public:
    explicit ExecutorInputImpl(const PbExecutor& message, uint32_t input_scope_level,
                               ExecutorType* executor) :
            ExecutorInputManager(message, input_scope_level), _executor(executor) {}

    virtual void on_input_ready() {
        _executor->on_input_ready(_keys);
    }

    virtual void on_input_done() {
        _executor->on_input_done();
    }

private:
    ExecutorType* _executor;
};

template<typename ExecutorType>
class ExecutorInputImpl<ExecutorType, false> {
public:
    explicit ExecutorInputImpl(const PbExecutor& message, uint32_t input_scope_level,
                               ExecutorType* executor) :
        _executor(executor), _input_scope_level(input_scope_level) {
        CHECK_EQ(message.input_size(), 0);
    }

    void setup(const std::map<std::string, Source*>& sources) {}

    Source* get(const std::string& identity) { return NULL; }

    uint32_t input_scope_level() const { return _input_scope_level; }

    uint32_t current_scope_level() { return _keys.size() - 1; }

    void begin_group(const toft::StringPiece& key) {
        _keys.push_back(key);
    }

    void finish_group() {
        if (current_scope_level() == _input_scope_level) {
            _executor->on_input_ready(_keys);
            _executor->on_input_done();
        }
        _keys.pop_back();
    }

private:
    ExecutorType* _executor;
    const uint32_t _input_scope_level;
    std::vector<toft::StringPiece> _keys;
};

class DispatcherManager {
public:
    DispatcherManager(const PbExecutor& message, DatasetManager* dataset_manager);
    virtual ~DispatcherManager();

    Dispatcher* get(const std::string& identity);

    PriorityDispatcher get_priority_dispatcher(const std::string& identity);

    Dispatcher* replace(const std::string& identity, Dispatcher* dispatcher);

    void begin_group(const toft::StringPiece& key, uint32_t target_scope_level);
    void finish_group();
    void close_prior_dispatchers(uint32_t priority);
    void done();

private:
    typedef boost::ptr_map<std::string, Dispatcher> DispatcherTable;
    typedef std::map<std::string, uint32_t> PriorityTable;

    DispatcherTable _dispatcher_table;
    PriorityTable _priority_table;

    std::vector<Dispatcher*> _dispatchers;  // arrange in priority order
    std::vector<uint32_t>  _prior_dispatcher_counts;

    uint32_t _last_priority;
};

template<bool kHasDispatcher> class ExecutorDispatcherImpl;

template<>
class ExecutorDispatcherImpl<true> : public DispatcherManager {
public:
    ExecutorDispatcherImpl(const PbExecutor& message, DatasetManager* dataset_manager) :
            DispatcherManager(message, dataset_manager) {}
};

template<>
class ExecutorDispatcherImpl<false> {
public:
    explicit ExecutorDispatcherImpl(const PbExecutor& message, DatasetManager* dataset_manager) {}

    Dispatcher* get(const std::string& identity) { return NULL; }

    PriorityDispatcher get_priority_dispatcher(const std::string& identity) { return PriorityDispatcher(); }

    Dispatcher* replace(const std::string& identity, Dispatcher* dispatcher) { return NULL; }

    void begin_group(const toft::StringPiece& key, uint32_t target_scope_level) {}

    void finish_group() {}

    void close_prior_dispatchers(uint32_t priority) {}

    void done() {}
};

template<bool kHasChild> class ExecutorChildImpl;

template<>
class ExecutorChildImpl<true> {
public:
    ExecutorChildImpl(const PbExecutor& message, const std::vector<Executor*>& childs) {
        _manager.Initialize(message, childs);
    }

    void setup(const std::map<std::string, Source*>& inputs) { _manager.Setup(inputs); }

    Source* get_source(const std::string& identity, int scope_level) {
        return _manager.GetSource(identity, scope_level);
    }

    void begin_group(const toft::StringPiece& key) { _manager.BeginGroup(key); }

    void finish_group() { _manager.FinishGroup(); }

private:
    SubExecutorManager _manager;
};

template<>
class ExecutorChildImpl<false> {
public:
    ExecutorChildImpl(const PbExecutor& message, const std::vector<Executor*>& childs) {}

    void setup(const std::map<std::string, Source*>& inputs) {}

    Source* get_source(const std::string& identity, int scope_level) { return NULL; }

    void begin_group(const toft::StringPiece& key) {}

    void finish_group() {}
};

enum ExecutorImplFlags {
    HAS_INPUT = 1,
    HAS_DISPATCHER = 2,
    HAS_CHILD = 4
};

template<typename RunnerType, uint8_t kFlags>
class ExecutorImpl : public ExecutorRunnerWrapper<RunnerType>, public ExecutorContext {
public:
    ExecutorImpl(uint32_t input_scope_level, const PbExecutor& message,
                 const std::vector<Executor*>& childs, DatasetManager* dataset_manager) :
            _message(message),
            _inputs(message, input_scope_level, this),
            _dispatchers(message, dataset_manager),
            _childs(message, childs) {}

    virtual void Setup(const std::map<std::string, Source*>& sources) {
        _inputs.setup(sources);
        _runner.setup(_message, this);

        std::map<std::string, Source*> child_inputs;
        for (int i = 0; i < _message.dispatcher_size(); ++i) {
            const std::string& identity = _message.dispatcher(i).identity();
            child_inputs[identity] = _dispatchers.get(identity)->GetSource(_message.scope_level());
        }
        _childs.setup(child_inputs);
    }

    virtual Source* GetSource(const std::string& identity, unsigned scope_level) {
        Dispatcher* dispatcher = _dispatchers.get(identity);
        if (dispatcher != NULL) {
            return dispatcher->GetSource(scope_level);
        } else {
            return _childs.get_source(identity, scope_level);
        }
    }

    virtual void BeginGroup(const toft::StringPiece& key) {
        _inputs.begin_group(key);
        _dispatchers.begin_group(key, _inputs.current_scope_level());
        _childs.begin_group(key);
    }

    virtual void FinishGroup() {
        if (_inputs.current_scope_level() != _inputs.input_scope_level()) {
            // when reach input scope level, we must wait until input is ready/done
            _childs.finish_group();
            _dispatchers.finish_group();
        }
        _inputs.finish_group();
    }

    // called at input scope level
    void on_input_ready(const std::vector<toft::StringPiece>& keys) {
        if (_inputs.input_scope_level() == _message.scope_level()) {
            // if at output scope level, runner may emit records in begin_group, so we
            // must notify childs and dispatcher to be prepared for outputs
            _childs.finish_group();
            _dispatchers.finish_group();
        }
        _runner.begin_group(keys);
    }

    // called at input scope level
    void on_input_done() {
        _runner.end_group();
        if (_inputs.input_scope_level() == _message.scope_level()) {
            // send done at output scope level
            _dispatchers.done();
        } else {
            // there should be no more outputs for input level, so we can safely leave
            // here
            _childs.finish_group();
            _dispatchers.finish_group();
        }
    }

protected:
    virtual RunnerType* runner() {
        return &_runner;
    }

    virtual Source* input(const std::string& identity) {
        return _inputs.get(identity);
    }

    virtual Dispatcher* output(const std::string& identity) {
        return _dispatchers.get(identity);
    }

    virtual PriorityDispatcher priority_output(const std::string& identity) {
        return _dispatchers.get_priority_dispatcher(identity);
    }

    virtual void close_prior_outputs(int priority) {
        if (priority < 0) {
            return;
        }
        _dispatchers.close_prior_dispatchers(priority);
    }

    virtual void begin_sub_group(const toft::StringPiece& key) {
        _dispatchers.begin_group(key, _message.scope_level());
        _childs.begin_group(key);
        _childs.finish_group();
        _dispatchers.finish_group();
    }

    virtual void end_sub_group() {
        _dispatchers.done();
    }

private:
    const PbExecutor _message;
    ExecutorInputImpl<ExecutorImpl, (kFlags & HAS_INPUT) != 0> _inputs;
    ExecutorDispatcherImpl<(kFlags & HAS_DISPATCHER) != 0> _dispatchers;
    ExecutorChildImpl<(kFlags & HAS_CHILD) != 0> _childs;

    RunnerType _runner;
};

}  // namespace internal

template<typename RunnerType> ExecutorRunnerWrapper<RunnerType>*
new_executor_by_runner(uint32_t input_scope_level, const PbExecutor& message,
                       const std::vector<Executor*>& childs, DatasetManager* dataset_manager) {
    uint8_t has_input = message.input_size() == 0 ? 0 : internal::HAS_INPUT;
    uint8_t has_dispatcher = message.dispatcher_size() == 0 ? 0 : internal::HAS_DISPATCHER;
    uint8_t has_child = message.child_size() == 0 ? 0 : internal::HAS_CHILD;

    switch (has_input | has_dispatcher | has_child) {
        case 0:
            return new internal::ExecutorImpl<RunnerType, 0>(
                input_scope_level, message, childs, dataset_manager);
        case 1:
            return new internal::ExecutorImpl<RunnerType, 1>(
                input_scope_level, message, childs, dataset_manager);
        case 2:
            return new internal::ExecutorImpl<RunnerType, 2>(
                input_scope_level, message, childs, dataset_manager);
        case 3:
            return new internal::ExecutorImpl<RunnerType, 3>(
                input_scope_level, message, childs, dataset_manager);
        case 4:
            return new internal::ExecutorImpl<RunnerType, 4>(
                input_scope_level, message, childs, dataset_manager);
        case 5:
            return new internal::ExecutorImpl<RunnerType, 5>(
                input_scope_level, message, childs, dataset_manager);
        case 6:
            return new internal::ExecutorImpl<RunnerType, 6>(
                input_scope_level, message, childs, dataset_manager);
        case 7:
            return new internal::ExecutorImpl<RunnerType, 7>(
                input_scope_level, message, childs, dataset_manager);
        default:
            LOG(FATAL) << "BUG!!!";
    }
    return NULL;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_EXECUTOR_IMPL_H_
