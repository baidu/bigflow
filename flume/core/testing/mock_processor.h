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

#ifndef FLUME_CORE_TESTING_MOCK_PROCESSOR_H_
#define FLUME_CORE_TESTING_MOCK_PROCESSOR_H_

#include <string>
#include <vector>

#include "boost/ptr_container/ptr_map.hpp"
#include "gmock/gmock.h"

#include "flume/core/processor.h"
#include "flume/core/testing/mock_base.h"
#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace flume {

class MockProcessor : public MockBase< ::baidu::flume::core::Processor, MockProcessor > {
public:
    MockProcessor() : m_emitter(NULL) {}

    MOCK_METHOD0(Synchronize, std::string());

    MOCK_METHOD3(BeginGroup, void(const std::vector<toft::StringPiece>&,
                                  const std::vector< ::baidu::flume::core::Iterator* >&,
                                  ::baidu::flume::core::Emitter*));

    // a smart version of BeginGroup, more easy to use
    MOCK_METHOD2(BeginGroup, void(const std::vector<std::string>&,
                                  const std::vector< ::baidu::flume::core::Iterator*>&));

    MOCK_METHOD2(Process, void(uint32_t, void*));

    MOCK_METHOD0(EndGroup, void());

    void Delegate(MockProcessor* mock) {
        // DELEGATE_METHOD3(mock, MockProcessor, BeginGroup);
        {
            using ::testing::_;
            using ::testing::AnyNumber;
            using ::testing::Invoke;

            // need special logicals to enable smart BeginGroup
            ON_CALL(*this, BeginGroup(_, _, _))
                .WillByDefault(Invoke(mock, &MockProcessor::DelegateBeginGroup));
            EXPECT_CALL(*this, BeginGroup(_, _, _)).Times(AnyNumber());
        }

        DELEGATE_METHOD0(mock, MockProcessor, Synchronize);
        DELEGATE_METHOD2(mock, MockProcessor, Process);
        DELEGATE_METHOD0(mock, MockProcessor, EndGroup);
    }

    bool Emit(void *object) {
        return m_emitter->Emit(object);
    }

    void Done() {
        m_emitter->Done();
    }

private:
    void DelegateBeginGroup(const std::vector<toft::StringPiece>& keys,
                            const std::vector< ::baidu::flume::core::Iterator* >& inputs,
                            ::baidu::flume::core::Emitter* emitter) {
        m_emitter = emitter;

        // Call smart version of BeginGroup
        std::vector<std::string> string_keys;
        for (size_t i = 0; i < keys.size(); ++i) {
            string_keys.push_back(keys[i].as_string());
        }
        this->BeginGroup(string_keys, inputs);
    }

private:
    ::baidu::flume::core::Emitter* m_emitter;
};

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_MOCK_PROCESSOR_H_
