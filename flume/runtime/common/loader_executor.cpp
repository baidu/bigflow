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

#include "flume/runtime/common/loader_executor.h"

#include <string>
#include <vector>

#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/emitter.h"
#include "flume/core/entity.h"
#include "flume/core/loader.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

using core::Entity;
using core::Loader;
using core::Objector;

LoaderExecutor::LoaderExecutor() : m_is_done(true) {}

LoaderExecutor::~LoaderExecutor() {}

void LoaderExecutor::Initialize(const PbExecutor& message, DatasetManager* dataset_manager) {
    m_message = message;
    m_dispatcher_manager.reset(new internal::DispatcherManager(m_message, dataset_manager));

    const PbLogicalPlanNode& node = message.logical_executor().node();
    m_output = m_dispatcher_manager->get(node.id());

    Entity<Loader> loader = Entity<Loader>::From(node.load_node().loader());
    m_loader.reset(loader.CreateAndSetup());
}

void LoaderExecutor::Setup(const std::map<std::string, Source*>& sources) {
}

Source* LoaderExecutor::GetSource(const std::string& id, unsigned scope_level) {
    CHECK_EQ(m_message.logical_executor().node().id(), id);
    return m_output->GetSource(scope_level);
}

void LoaderExecutor::BeginGroup(const toft::StringPiece& key) {
    m_keys.push_back(key);
    m_output->BeginGroup(key);
}

void LoaderExecutor::FinishGroup() {
    m_output->FinishGroup();
    if (m_keys.size() == m_message.scope_level() + 1) {
        m_is_done = false;
        if (m_output->IsAcceptMore()) {
            const std::string split = m_keys.back().as_string();
            PbSplit message = Loader::DecodeSplit(split);
            m_loader->Load(message.has_raw_split() ? message.raw_split() : split, this);
        }
        Done();
    }
    m_keys.pop_back();
}

bool LoaderExecutor::Emit(void* object) {
    if (!m_is_done) {
        return m_output->EmitObject(object);
    }
    return false;
}

void LoaderExecutor::Done() {
    if (!m_is_done) {
        m_is_done = true;
        m_output->Done();
    }
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
