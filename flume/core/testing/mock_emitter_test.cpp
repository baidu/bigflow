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
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#include "flume/core/testing/mock_emitter.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"

namespace baidu {
namespace flume {

using ::testing::InSequence;

TEST(MockEmitterTest, Basic) {
    int inputs[] = {1, 2, 3, 4, 5};

    MockEmitter<int> mock;
    {
        InSequence in_sequence;

        for (size_t i = 0; i < TOFT_ARRAY_SIZE(inputs); ++i) {
            EXPECT_CALL(mock, EmitValue(inputs[i]));
        }
        EXPECT_CALL(mock, Done());
    }

    core::Emitter* emitter = &mock;
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(inputs); ++i) {
        ASSERT_TRUE(emitter->Emit(&inputs[i]));
    }
    emitter->Done();
}

TEST(MockEmitterTest, Void) {
    int inputs[] = {1, 2, 3, 4, 5};

    MockEmitter<> mock;
    {
        InSequence in_sequence;

        for (size_t i = 0; i < TOFT_ARRAY_SIZE(inputs); ++i) {
            EXPECT_CALL(mock, EmitValue(&inputs[i]));
        }
        EXPECT_CALL(mock, Done());
    }

    core::Emitter* emitter = &mock;
    for (size_t i = 0; i < TOFT_ARRAY_SIZE(inputs); ++i) {
        ASSERT_TRUE(emitter->Emit(&inputs[i]));
    }
    emitter->Done();
}

}  // namespace flume
}  // namespace baidu
