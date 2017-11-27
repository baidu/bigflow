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

#include "flume/runtime/common/sinker_executor.h"

#include <string>
#include <vector>

#include "toft/base/closure.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/sinker.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/dispatcher.h"
#include "flume/runtime/executor.h"

namespace baidu {
namespace flume {
namespace runtime {

using core::Entity;
using core::Sinker;

void SinkerExecutor::Initialize(const PbExecutor& message) {
    m_message = message;
}

void SinkerExecutor::Setup(const std::map<std::string, Source*>& sources) {
    const PbSinkNode& sink_node = m_message.logical_executor().node().sink_node();
    Entity<Sinker> entity = Entity<Sinker>::From(sink_node.sinker());
    m_sinker.reset(entity.CreateAndSetup());

    std::map<std::string, Source*>::const_iterator ptr = sources.find(sink_node.from());
    CHECK(ptr != sources.end());
    ptr->second->RequireStream(Source::REQUIRE_OBJECT,
        toft::NewPermanentClosure(this, &SinkerExecutor::OnInputCome),
        toft::NewPermanentClosure(m_sinker.get(), &Sinker::Close)
    );
}

Source* SinkerExecutor::GetSource(const std::string& id, unsigned scope_level) {
    CHECK(true) << "SinkerExecutor do not have output!";
    return NULL;
}

void SinkerExecutor::BeginGroup(const toft::StringPiece& key) {
    m_keys.push_back(key);
}

void SinkerExecutor::FinishGroup() {
    if (m_keys.size() == m_message.scope_level() + 1) {
        m_sinker->Open(m_keys);
    }
    m_keys.pop_back();
}

void SinkerExecutor::OnInputCome(const std::vector<toft::StringPiece>& keys,
                                 void* object, const toft::StringPiece& binary) {
    m_sinker->Sink(object);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
