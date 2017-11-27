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

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "boost/foreach.hpp"
#include "glog/logging.h"

#include "flume/core/testing/processor_test_helper.h"

namespace baidu {
namespace flume {
namespace core {

ProcessorTestHelper::ProcessorTestHelper(const Entity<Processor>& entity, Emitter* emitter) {
    m_processor = entity.CreateAndSetup();
    m_emitter = emitter;

    m_processor_deleter.reset(m_processor);
}

ProcessorTestHelper::ProcessorTestHelper(Processor* processor, Emitter* emitter)
        : m_processor(processor), m_emitter(emitter) {}

void ProcessorTestHelper::AddKey(const std::string& key) {
    m_keys.push_back(key);
}

void ProcessorTestHelper::AddPreparedInput(uint32_t index, void* object) {
    m_inputs[index].push_back(object);
}

void ProcessorTestHelper::BeginGroup(uint32_t inputs_number) {
    std::vector<toft::StringPiece> keys(m_keys.begin(), m_keys.end());
    std::vector<core::Iterator*> inputs(inputs_number, NULL);

    // transform from m_inputs to inputs
    typedef std::map<uint32_t, std::vector<void*> > Map;
    for (Map::iterator it = m_inputs.begin(); it != m_inputs.end(); ++it) {
        IteratorImpl *input = new IteratorImpl(it->second.begin(), it->second.end());
        m_inputs_deleter.push_back(input);

        uint32_t index = it->first;
        CHECK(index < inputs_number);
        inputs[index] = input;
    }

    return m_processor->BeginGroup(keys, inputs, m_emitter);
}

void ProcessorTestHelper::Process(uint32_t index, void* object) {
    return m_processor->Process(index, object);
}

void ProcessorTestHelper::EndGroup() {
    m_processor->EndGroup();

    m_keys.clear();
    m_inputs.clear();
}

}  // namespace core
}  // namespace flume
}  // namespace baidu
