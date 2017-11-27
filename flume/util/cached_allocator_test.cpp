/***************************************************************************
 *
 * Copyright (c) 2014 Baidu, Inc. All Rights Reserved.
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
// Author: Liu Cheng <liucheng02@baidu.com>

#include "gtest/gtest.h"

#include <sys/time.h>
#include "flume/util/cached_allocator.h"

namespace baidu {
namespace flume {

class CachedObject : public CachedAllocator<CachedObject> {
public:
};

class Object {
public:
};

TEST(CachedAllocatortTest, AllocateAndRelease) {
    CachedObject* t = new CachedObject();
    delete t;
    ASSERT_EQ(1, MemoryPieceStore::Instance()->allocate_count());
    ASSERT_EQ(1, MemoryPieceStore::Instance()->release_count());

    std::vector<CachedObject*> objects;
    for (size_t i = 0; i < MemoryPieceStore::kMaxCacheCount * 10; ++i) {
        objects.push_back(new CachedObject());
    }
    ASSERT_EQ(MemoryPieceStore::kMaxCacheCount * 10 + 1,
              MemoryPieceStore::Instance()->allocate_count());
    for (size_t i = 0; i < objects.size(); ++i) {
        delete objects[i];
    }
    ASSERT_EQ(MemoryPieceStore::kMaxCacheCount * 10 + 1,
              MemoryPieceStore::Instance()->release_count());
}

TEST(CachedAllocatortTest, DeleteNull) {
    CachedObject* t = NULL;
    delete t;
}

// Test result show that, cached object brings about 17x performance gains
TEST(CachedAllocatortTest, PressureTest) {
    struct timeval t1, t2, t3;
    gettimeofday(&t1, NULL);
    for (size_t i = 0; i < 1000 * 1000; ++i) {
        CachedObject* t = new CachedObject();
        delete t;
    }
    gettimeofday(&t2, NULL);
    for (size_t i = 0; i < 1000 * 1000; ++i) {
        Object* t = new Object();
        delete t;
    }
    gettimeofday(&t3, NULL);
    int cost1_us = (t2.tv_sec - t1.tv_sec) * 1000 * 1000 + (t2.tv_usec - t1.tv_usec);
    int cost2_us = (t3.tv_sec - t2.tv_sec) * 1000 * 1000 + (t3.tv_usec - t2.tv_usec);
    LOG(INFO) << "cost1:" << cost1_us << ", cost2:" << cost2_us;
}

}  // namespace flume
}  // namespace baidu
