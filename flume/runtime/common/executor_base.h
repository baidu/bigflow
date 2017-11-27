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

#ifndef FLUME_RUNTIME_COMMON_EXECUTOR_BASE_H_
#define FLUME_RUNTIME_COMMON_EXECUTOR_BASE_H_

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "boost/ptr_container/ptr_map.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/scoped_ptr.hpp"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/sub_executor_manager.h"
#include "flume/runtime/executor.h"
#include "flume/util/bitset.h"

namespace baidu {
namespace flume {
namespace runtime {

class DatasetManager;
class Dispatcher;
class ExecutorBase;
class ExecutorCore;
class GeneralDispatcher;
class Source;

class ExecutorCore {
public:
    virtual ~ExecutorCore() {}

    virtual void Setup(const PbExecutor& message, ExecutorBase* base) = 0;

    // called when all inputs is ready
    virtual void BeginGroup(const std::vector<toft::StringPiece>& keys) = 0;

    // called after all inputs is done. maybe called from by Source::Handle::Done
    virtual void EndGroup() = 0;
};

class ExecutorBase : public Executor {
public:
    ExecutorBase();
    explicit ExecutorBase(ExecutorCore* core);
    virtual ~ExecutorBase();

    void Initialize(const PbExecutor& message, const std::vector<Executor*>& childs,
                    uint32_t father_scope_level, DatasetManager* dataset_manager);

    ExecutorCore* GetExecutorCore();

    // implement Executor
    virtual void Setup(const std::map<std::string, Source*>& sources);

    virtual Source* GetSource(const std::string& id, unsigned scope_level);

    virtual void BeginGroup(const toft::StringPiece& key);

    virtual void FinishGroup();

    // for ExecutorCore
    virtual Source* GetInput(const std::string& id);
    virtual Dispatcher* GetOutput(const std::string& id);

    virtual void BeginSubGroup(const toft::StringPiece& key);
    virtual void EndSubGroup();

private:
    void BeginOutputsAndChilds(const toft::StringPiece& key, uint32_t target_scope_level);
    void FinishChildsAndOutputs();

    void OnInputDone(int index, toft::Closure<void ()>* done);

    void CloseOutputs();
    void CheckInputsDone();

private:
    class HandleImpl;
    class SourceImpl;

    typedef util::Bitset Bitset;
    typedef boost::ptr_map<std::string, SourceImpl> InputMap;

private:
    boost::scoped_ptr<ExecutorCore> m_core;

    PbExecutor m_message;
    uint32_t m_input_scope_level;

    boost::ptr_map<std::string, SourceImpl> m_inputs;
    boost::ptr_vector<Dispatcher> m_dispatchers;
    boost::ptr_vector<toft::ClosureBase> m_delegate_dones;

    SubExecutorManager m_childs;
    std::map<std::string, Source*> m_child_inputs;
    std::map<std::string, Dispatcher*> m_dispatcher_table;

    std::vector<toft::StringPiece> m_keys;
    Bitset m_active_inputs;
};

template<class CoreType>
class ExecutorImpl : public ExecutorBase {
public:
    ExecutorImpl() : ExecutorBase(new CoreType()) {}

    // ExectorImpl will manage the lifetime of the executor_core
    explicit ExecutorImpl(CoreType* core) : ExecutorBase(core) {}
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_COMMON_EXECUTOR_BASE_H_
