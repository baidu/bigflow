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
// Author: Wen Xiang <wenxiang@baidu.com>

#include "flume/runtime/util/serialize_buffer.h"

#include <cstring>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::InSequence;
using ::testing::Return;
using ::testing::Ge;
using ::testing::NotNull;

class MockFn {
public:
    MOCK_METHOD3(Serialize, uint32_t(void*, char*, uint32_t));
};

TEST(SerializeBufferTest, Empty) {
    SerializeBuffer buffer(4096);
}

TEST(SerializeBufferTest, AllocateWithPreparedBuffer) {
    SerializeBuffer buffer(4096);
    void* object = reinterpret_cast<void*>(0x1ul);

    MockFn fn;
    EXPECT_CALL(fn, Serialize(object, NotNull(), 4096)).WillOnce(Return(1));

    SerializeBuffer::Guard guard;
    guard = buffer.Allocate(&fn, &MockFn::Serialize, object);
    EXPECT_EQ(1, guard.ref().size());
}

TEST(SerializeBufferTest, AllocateWithTemporaryBuffer) {
    SerializeBuffer buffer(4096);
    void* object = reinterpret_cast<void*>(0x1ul);

    MockFn fn;
    {
        InSequence in_sequence;
        EXPECT_CALL(fn, Serialize(object, NotNull(), 4096)).WillOnce(Return(4097));
        EXPECT_CALL(fn, Serialize(object, NotNull(), 4097)).WillOnce(Return(4097));
    }

    SerializeBuffer::Guard guard;
    guard = buffer.Allocate(&fn, &MockFn::Serialize, object);
    EXPECT_EQ(4097, guard.ref().size());
}

TEST(SerializeBufferTest, CopyWithPreparedBuffer) {
    toft::StringPiece src("hello world");
    SerializeBuffer buffer(4096);

    SerializeBuffer::Guard guard;
    guard = buffer.Copy(src);

    EXPECT_EQ(src, guard.ref());
    EXPECT_NE(src.data(), guard.ref().data());
}

TEST(SerializeBufferTest, CopyWithTemporaryBuffer) {
    std::string src(4097, '1');
    SerializeBuffer buffer(4096);

    SerializeBuffer::Guard guard;
    guard = buffer.Copy(src);

    EXPECT_EQ(src, guard.ref());
    EXPECT_NE(src.data(), guard.ref().data());
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
