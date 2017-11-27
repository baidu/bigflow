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

#include "flume/runtime/common/executor_base.h"

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "boost/ptr_container/ptr_map.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/general_dispatcher.h"
#include "flume/runtime/common/single_dispatcher.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

// a delegate for real source
class ExecutorBase::SourceImpl : public Source {
public:
    SourceImpl() : m_base(NULL), m_source(NULL) {}

    void Initialize(ExecutorBase* base) {
        m_base = base;
    }

    void Setup(Source* source) {
        m_source = source;
    }

protected:
    virtual Handle* RequireStream(uint32_t flag, StreamCallback* callback, DoneCallback* done) {
        int index = m_base->m_active_inputs.size();
        m_base->m_active_inputs.push_back(false);
        m_base->m_delegate_dones.push_back(done);
        return m_source->RequireStream(
                flag, callback,
                toft::NewPermanentClosure(m_base, &ExecutorBase::OnInputDone, index, done)
        );
    }

    virtual Handle* RequireIterator(IteratorCallback* iterator_callback,
                                    DoneCallback* done) {
        int index = m_base->m_active_inputs.size();
        m_base->m_active_inputs.push_back(false);
        m_base->m_delegate_dones.push_back(done);
        return m_source->RequireIterator(
                iterator_callback,
                toft::NewPermanentClosure(m_base, &ExecutorBase::OnInputDone, index, done)
        );
    }

private:
    ExecutorBase* m_base;
    Source* m_source;
};

ExecutorBase::ExecutorBase() {
}

ExecutorBase::ExecutorBase(ExecutorCore* core) {
    m_core.reset(core);
}

ExecutorBase::~ExecutorBase() {
}

void ExecutorBase::Initialize(const PbExecutor& message, const std::vector<Executor*>& childs,
                              uint32_t input_scope_level, DatasetManager* dataset_manager) {
    m_message = message;
    m_input_scope_level = input_scope_level;
    m_childs.Initialize(message, childs);

    for (int i = 0; i < message.input_size(); ++i) {
        m_inputs[message.input(i)].Initialize(this);
    }

    for (int i = 0; i < message.dispatcher_size(); ++i) {
        const PbExecutor::Dispatcher& config = message.dispatcher(i);

        std::auto_ptr<Dispatcher> dispatcher;
        dispatcher.reset(new GeneralDispatcher(config.identity(), message.scope_level()));
        dispatcher->SetDatasetManager(dataset_manager);

        m_dispatcher_table[config.identity()] = dispatcher.get();
        m_dispatchers.push_back(dispatcher);
    }

    for (int i = 0; i < message.child_size(); ++i) {
        const PbExecutor& child = message.child(i);
        for (int j = 0; j < child.input_size(); ++j) {
            const std::string& identity = child.input(j);

            if (m_inputs.count(identity) != 0) {
                m_child_inputs[identity] = &m_inputs[identity];
            }

            if (m_dispatcher_table.count(identity) != 0) {
                m_child_inputs[identity] =
                        m_dispatcher_table[identity]->GetSource(message.scope_level());
            }
        }
    }
}

void ExecutorBase::Setup(const std::map<std::string, Source*>& sources) {
    for (InputMap::iterator ptr = m_inputs.begin(); ptr != m_inputs.end(); ++ptr) {
        CHECK_EQ(1, sources.count(ptr->first));
        ptr->second->Setup(sources.find(ptr->first)->second);
    }
    m_core->Setup(m_message, this);
    m_childs.Setup(m_child_inputs);
}

Source* ExecutorBase::GetSource(const std::string& id, unsigned scope_level) {
    if (m_dispatcher_table.count(id) != 0) {
        return m_dispatcher_table[id]->GetSource(scope_level);
    } else {
        return m_childs.GetSource(id, scope_level);
    }
}

void ExecutorBase::BeginGroup(const toft::StringPiece& key) {
    m_keys.push_back(key);
    if (m_keys.size() == m_input_scope_level + 1) {
        m_active_inputs.set();
    }
    BeginOutputsAndChilds(key, m_keys.size() - 1);
}

void ExecutorBase::FinishGroup() {
    if (m_keys.size() < m_input_scope_level + 1) {
        FinishChildsAndOutputs();
        m_keys.pop_back();
        return;
    }

    if (m_keys.size() == m_message.scope_level() + 1) {
        FinishChildsAndOutputs();
    }
    m_core->BeginGroup(m_keys);
    CheckInputsDone();
}

inline void ExecutorBase::CheckInputsDone() {
    if (m_active_inputs.any()) {
        return;
    }

    m_core->EndGroup();
    if (m_keys.size() == m_message.scope_level() + 1) {
        CloseOutputs();
    } else {
        FinishChildsAndOutputs();
    }
    m_keys.pop_back();
}

Source* ExecutorBase::GetInput(const std::string& id) {
    CHECK_EQ(1, m_inputs.count(id)) << "Cannot get input: " << id;
    return &m_inputs[id];
}

Dispatcher* ExecutorBase::GetOutput(const std::string& id) {
    CHECK_EQ(1, m_dispatcher_table.count(id)) << "Cannot get output: " << id;
    return m_dispatcher_table[id];
}

void ExecutorBase::BeginSubGroup(const toft::StringPiece& key) {
    DCHECK_EQ(m_keys.size(), m_message.scope_level());
    BeginOutputsAndChilds(key, m_message.scope_level());
    FinishChildsAndOutputs();
}

void ExecutorBase::EndSubGroup() {
    DCHECK_EQ(m_keys.size(), m_message.scope_level());
    CloseOutputs();
}

void ExecutorBase::BeginOutputsAndChilds(const toft::StringPiece& key,
                                         uint32_t target_scope_level) {
    for (size_t i = 0; i < m_dispatchers.size(); ++i) {
        Dispatcher* dispatcher = &m_dispatchers[i];
        if (dispatcher->GetScopeLevel() != target_scope_level) {
            dispatcher->BeginGroup(key);
        }
        DCHECK_EQ(target_scope_level, dispatcher->GetScopeLevel());
    }
    m_childs.BeginGroup(key);
}

void ExecutorBase::FinishChildsAndOutputs() {
    m_childs.FinishGroup();
    for (size_t i = 0; i < m_dispatchers.size(); ++i) {
        m_dispatchers[i].FinishGroup();
    }
}

void ExecutorBase::CloseOutputs() {
    for (size_t i = 0; i < m_dispatchers.size(); ++i) {
        m_dispatchers[i].Done();
    }
}

inline void ExecutorBase::OnInputDone(int index, Source::DoneCallback* callback) {
    callback->Run();

    m_active_inputs[index] = false;
    CheckInputsDone();
}

ExecutorCore* ExecutorBase::GetExecutorCore() {
    return m_core.get();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
