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
//
// This file declares MockEmitter, a mock class for testing.

#ifndef FLUME_CORE_TESTING_MOCK_EMITTER_H_
#define FLUME_CORE_TESTING_MOCK_EMITTER_H_

#include "gmock/gmock.h"

#include "flume/core/emitter.h"

namespace baidu {
namespace flume {

// MockEmitter is designed to be used with gmock. For example:
//
//      MockEmitter<int> mock;
//      EXPECT_CALL(mock, OnEmit(1));
//
//      int result = 1;
//      mock.Emit(&result);
//
// |see https://code.google.com/p/googlemock/wiki/CookBook to learn gmock.

template<typename T = void>
class MockEmitter;

template<typename T>
class MockEmitter : public core::Emitter {
public:
    MockEmitter() {
        using ::testing::_;
        using ::testing::Return;

        ON_CALL(*this, EmitValue(_))
            .WillByDefault(Return(true));
    }

    // Gmock predicates can be used on OnEmit instead
    virtual bool Emit(void* object) {
        return EmitValue(*static_cast<T*>(object));
    }


    MOCK_METHOD0(Done, void());
    MOCK_METHOD1_T(EmitValue, bool(const T& object));
};

template<>
class MockEmitter<void> : public core::Emitter {
public:
    MockEmitter() {
        using ::testing::_;
        using ::testing::Return;

        ON_CALL(*this, EmitValue(_))
            .WillByDefault(Return(true));
    }

    // Gmock predicates can be used on OnEmit instead
    virtual bool Emit(void* object) {
        return EmitValue(object);
    }

    MOCK_METHOD0(Done, void());
    MOCK_METHOD1(EmitValue, bool(void* object));
};

ACTION_P3(EmitAndExpect, fn, object, result) {
    CHECK_EQ(result, fn->Emit(object));
}

ACTION_P(EmitDone, fn) {
    fn->Done();
}

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_MOCK_EMITTER_H_
