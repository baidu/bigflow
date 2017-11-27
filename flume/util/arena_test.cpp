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
// Author: Xue Kang <bigflow-opensource@baidu.com>

#include <string>

#include "boost/bind.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"

#include "flume/util/arena.h"

namespace baidu {
namespace flume {
namespace util {

using ::testing::InSequence;
using ::testing::Return;
using ::testing::Ge;
using ::testing::NotNull;

static const int kMinBlockSize = 4096;              // 4k page; should be 2^N
static const int kMaxBlockSize = 64 * 1024 * 1024;  // 64M; should be 2^N

TEST(ArenaTest, Initial) {
    Arena arena;

    arena.Reset();
    EXPECT_TRUE(arena.current_block() == NULL);
    EXPECT_EQ(0u, arena.total_reserved_bytes());

    EXPECT_TRUE(arena.remained_buffer() != NULL);
    EXPECT_TRUE(arena.current_block() != NULL);
    EXPECT_EQ(kMinBlockSize, arena.remained_buffer_size());
    EXPECT_EQ(kMinBlockSize, arena.total_reserved_bytes());

    arena.Reset();
    EXPECT_TRUE(arena.current_block() != NULL);
    EXPECT_EQ(kMinBlockSize, arena.total_reserved_bytes());
}

TEST(ArenaTest, ReserveBytes) {
    Arena arena;
    char* ptr;

    ptr = arena.ReserveBytes(kMinBlockSize / 2);
    EXPECT_EQ(ptr, arena.remained_buffer());
    EXPECT_EQ(kMinBlockSize, arena.remained_buffer_size());
    EXPECT_EQ(kMinBlockSize, arena.total_reserved_bytes());

    ptr = arena.ReserveBytes(kMinBlockSize + 1);
    EXPECT_EQ(ptr, arena.remained_buffer());
    EXPECT_EQ(2 * kMinBlockSize, arena.remained_buffer_size());
    EXPECT_EQ(2 * kMinBlockSize, arena.total_reserved_bytes());

    ptr = arena.ReserveBytes(kMaxBlockSize / 2);
    EXPECT_EQ(ptr, arena.remained_buffer());
    EXPECT_EQ(kMaxBlockSize / 2, arena.remained_buffer_size());
    EXPECT_EQ(kMaxBlockSize / 2, arena.total_reserved_bytes());

    ptr = arena.ReserveBytes(kMaxBlockSize - 1);
    EXPECT_EQ(ptr, arena.remained_buffer());
    EXPECT_EQ(kMaxBlockSize, arena.remained_buffer_size());
    EXPECT_EQ(kMaxBlockSize, arena.total_reserved_bytes());

    ptr = arena.ReserveBytes(kMaxBlockSize + 1);
    EXPECT_EQ(ptr, arena.remained_buffer());
    EXPECT_EQ(kMaxBlockSize + kMinBlockSize, arena.remained_buffer_size());
    EXPECT_EQ(kMaxBlockSize + kMinBlockSize, arena.total_reserved_bytes());
}

TEST(ArenaTest, AllocateBytes) {
    Arena arena;
    char* ptr = NULL;

    // allocate kMinBlockSize / 4 bytes
    ptr = arena.AllocateBytes(kMinBlockSize / 4);
    EXPECT_TRUE(ptr != NULL);
    EXPECT_EQ(arena.current_block(), ptr);
    EXPECT_EQ(kMinBlockSize, arena.total_reserved_bytes());

    EXPECT_EQ(arena.current_block() + kMinBlockSize / 4, arena.remained_buffer());
    EXPECT_EQ(kMinBlockSize - kMinBlockSize / 4, arena.remained_buffer_size());

    // allocate kMinBlockSize / 2 bytes
    ptr = arena.AllocateBytes(kMinBlockSize / 2);
    EXPECT_TRUE(ptr != NULL);
    EXPECT_EQ(arena.current_block() + kMinBlockSize / 4, ptr);
    EXPECT_EQ(kMinBlockSize, arena.total_reserved_bytes());

    EXPECT_EQ(arena.current_block() + kMinBlockSize * 3 / 4, arena.remained_buffer());
    EXPECT_EQ(kMinBlockSize / 4, arena.remained_buffer_size());

    // allocate kMinBlockSize bytes
    ptr = arena.AllocateBytes(kMinBlockSize);
    EXPECT_TRUE(ptr != NULL);
    EXPECT_EQ(arena.current_block(), ptr);
    EXPECT_EQ(kMinBlockSize * 3, arena.total_reserved_bytes());

    EXPECT_EQ(arena.current_block() + kMinBlockSize, arena.remained_buffer());
    EXPECT_EQ(kMinBlockSize, arena.remained_buffer_size());

    // reset
    arena.Reset();
    EXPECT_TRUE(arena.current_block() != NULL);
    EXPECT_EQ(kMinBlockSize, arena.total_reserved_bytes());
}

template<size_t N>
struct Array {
    char data[N];
};

TEST(ArenaTest, AllocateType) {
    Arena arena;

    arena.Allocate< Array<9> >();
    EXPECT_EQ(9, arena.total_reserved_bytes() - arena.remained_buffer_size());

    arena.Allocate<uint32_t>();
    EXPECT_EQ(16, arena.total_reserved_bytes() - arena.remained_buffer_size());
}

class MockFn {
public:
    MOCK_METHOD3(Serialize, uint32_t(void*, char*, uint32_t));
};

TEST(ArenaTest, AllocateFn) {
    Arena arena;

    size_t kSerializeSize = kMinBlockSize + 1;
    MockFn fn;
    {
        InSequence in_sequence;

        EXPECT_CALL(fn, Serialize(NULL, arena.remained_buffer(), arena.remained_buffer_size()))
            .WillOnce(Return(kSerializeSize));
        EXPECT_CALL(fn, Serialize(NULL, NotNull(), Ge(kSerializeSize)))
            .WillOnce(Return(kSerializeSize));
    }

    toft::StringPiece ret = arena.Allocate(
        boost::bind(&MockFn::Serialize, &fn, static_cast<void*>(NULL), _1, _2)
    );
    EXPECT_EQ(kSerializeSize, ret.size());
    EXPECT_LT(kMinBlockSize, arena.total_reserved_bytes());
    EXPECT_EQ(ret.data(), arena.current_block());
}

TEST(ArenaTest, AllocateStringPiece) {
    Arena arena;

    toft::StringPiece origin = "hello world";
    toft::StringPiece copy = arena.Allocate(origin);
    EXPECT_EQ(origin, copy);
    EXPECT_NE(origin.data(), copy.data());
    EXPECT_EQ(copy.data(), arena.current_block());
}

} // namespace util
} // namespace flume
} // namespace baidu

