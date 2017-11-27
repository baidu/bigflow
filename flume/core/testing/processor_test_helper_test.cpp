/************************************************************************
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
//
// This file declares MockEmitter, a mock class for testing.

#include "flume/core/testing/processor_test_helper.h"

#include "boost/lexical_cast.hpp"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/system/memory/unaligned.h"

#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace flume {
namespace core {

using ::testing::_;
using ::testing::ElementsAreArray;
using ::testing::Invoke;

class SumProcessor : public Processor {
public:
    virtual void Setup(const std::string& config) {
    }

    virtual void BeginGroup(const std::vector<toft::StringPiece>& keys,
                            const std::vector<Iterator*>& inputs,
                            Emitter* emitter) {
        ASSERT_EQ(2u, inputs.size());
        ASSERT_TRUE(inputs[1] != NULL);

        m_emitter = emitter;
        m_sum = 0;

        Iterator* input = inputs[1];
        input->Reset();
        while (input->HasNext()) {
            m_sum += toft::GetUnaligned<int32_t>(input->NextValue());
        }
        input->Done();
    }

    virtual void Process(uint32_t index, void* object) {
        ASSERT_EQ(0u, index);

        m_sum += boost::lexical_cast<int32_t>(static_cast<char*>(object));
    }

    virtual void EndGroup() {
        m_emitter->Emit(&m_sum);
        m_emitter->Done(); // calling Done() is not needed, just for illustration
    }

private:
    Emitter* m_emitter;
    int64_t m_sum;
};

class MockSumProcessor : public SumProcessor {
public:
    MockSumProcessor() {
        ON_CALL(*this, BeginGroup(_, _, _))
            .WillByDefault(Invoke(this, &MockSumProcessor::RealBeginGroup));
    }

    MOCK_METHOD3(BeginGroup, void(const std::vector<toft::StringPiece>&,
                                  const std::vector<Iterator*>&, Emitter*));

    void RealBeginGroup(const std::vector<toft::StringPiece>& keys,
                        const std::vector<Iterator*>& inputs,
                        Emitter* emitter) {
        SumProcessor::BeginGroup(keys, inputs, emitter);
    }
};


TEST(ProcessorTestHelper, Empty) {
    Entity<Processor> entity = Entity<Processor>::Of<SumProcessor>("");
    ProcessorTestHelper helper(entity, NULL);
}

TEST(ProcessorTestHelper, Basic) {
    const char* keys[] = {"key1", "key2"};
    int32_t prepared_inputs[] = {1, 2, 3};
    const char* stream_inputs[] = {"4", "5"};

    MockEmitter<int64_t> emitter;
    EXPECT_CALL(emitter, EmitValue(15));
    EXPECT_CALL(emitter, Done());

    MockSumProcessor processor;
    EXPECT_CALL(processor, BeginGroup(ElementsAreArray(keys), _, _));

    ProcessorTestHelper helper(&processor, &emitter);
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(keys); ++i) {
        helper.AddKey(keys[i]);
    }
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(prepared_inputs); ++i) {
        helper.AddPreparedInput(1, &prepared_inputs[i]);
    }

    helper.BeginGroup(2);
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(stream_inputs); ++i) {
        helper.Process(0, const_cast<char*>(stream_inputs[i]));
    }
    helper.EndGroup();
}

}  // namespace core
}  // namespace flume
}  // namespace baidu
