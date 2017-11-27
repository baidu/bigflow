/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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
// Author: Wang Cong <bigflow-opensource@baidu.com>

#include "flume/runtime/spark/kv_buffer.h"

#include <string>

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

void BasicOperationsRoutine(KVBuffer& kv_buffer) {
    EXPECT_FALSE(kv_buffer.has_next());
    {
        std::string keys[4] = {"key1", "key2", "key3", "key4"};
        std::string values[4] = {"value1", "value2", "value3", "value4"};
        for (int i = 0; i < 4; ++i) {
            kv_buffer.put(keys[i], values[i]);
        }
    }

    {
        std::string keys[4] = {"key1", "key2", "key3", "key4"};
        std::string values[4] = {"value1", "value2", "value3", "value4"};
        for (int i = 0; i < 4; ++i) {
            EXPECT_TRUE(kv_buffer.has_next());
            kv_buffer.next();

            toft::StringPiece key;
            toft::StringPiece value;
            kv_buffer.key(&key);
            kv_buffer.value(&value);
            EXPECT_EQ(keys[i], key);
            EXPECT_EQ(values[i], value);
        }
        EXPECT_FALSE(kv_buffer.has_next());
    }
}

TEST(KVBufferTest, BasicOperations) {
    KVBuffer kv_buffer(1024);
    BasicOperationsRoutine(kv_buffer);
}

TEST(KVBufferTest, ResetBuffer) {
    KVBuffer kv_buffer(1024);
    BasicOperationsRoutine(kv_buffer);

    kv_buffer.reset();
    BasicOperationsRoutine(kv_buffer);
}

TEST(KVBufferTest, Grow) {
    KVBuffer kv_buffer(16);
    BasicOperationsRoutine(kv_buffer);
    kv_buffer.reset();
    BasicOperationsRoutine(kv_buffer);
}

TEST(KVBufferDeathTest, HasNoNext) {
    KVBuffer kv_buffer(16);
    kv_buffer.put("SomeKey", "SomeValue");
    EXPECT_TRUE(kv_buffer.has_next());
    kv_buffer.next();
    EXPECT_FALSE(kv_buffer.has_next());
    EXPECT_DEATH(kv_buffer.next(), "Has no next");
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
