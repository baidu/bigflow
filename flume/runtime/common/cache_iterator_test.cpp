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
// Author: Zhang Yuncong (bigflow-opensource@baidu.com)

#include "flume/runtime/common/cache_iterator.h"

#include <cstring>
#include <iterator>
#include <list>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/testing/string_objector.h"
#include "flume/runtime/common/file_cache_manager.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::InSequence;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;

std::vector<std::string> keys(const char* key1,
                              const char* key2 = NULL,
                              const char* key3 = NULL,
                              const char* key4 = NULL) {
    std::vector<std::string> ret;
    const char* keys[] = {key1, key2, key3, key4};
    for(int i = 0; i != TOFT_ARRAY_SIZE(keys); ++i) {
        if (keys[i]) {
            ret.push_back(keys[i]);
        }
    }
    return ret;
}

std::vector<toft::StringPiece> to_piece(const std::vector<std::string>& strs) {
    std::vector<toft::StringPiece> ret(strs.begin(), strs.end());
    return ret;
}


struct Record {
    std::vector<std::string> keys;
    std::string value;
};

std::string concat_record(const Record& record) {
    std::string ret;
    for(size_t i = 0; i != record.keys.size(); ++i) {
        ret += record.keys[i];
        ret.resize(ret.size() + 1);
    }
    ret += record.value;
    return ret;
}

bool operator < (const Record& left, const Record& right) {
    return concat_record(left) < concat_record(right);
}

bool operator == (const Record& left, const Record& right) {
    return !(left < right) && !(right < left);
}

std::string as_string(const toft::StringPiece& piece) {
    return piece.as_string();
}

Record record(std::vector<toft::StringPiece> keys, const std::string* value) {
    Record ret;
    std::transform(keys.begin(), keys.end(), std::back_inserter(ret.keys), as_string);
    ret.value = *value;
    return ret;
}

Record record(std::vector<std::string> keys, std::string value) {
    Record ret;
    ret.keys = keys;
    ret.value = value;
    return ret;
}

void write_doodle(const std::string& dir, const std::string& task_id, const std::string& prefix) {
    FileCacheManager manager(dir, task_id);
    CacheManager::Writer* writer = manager.GetWriter("node-1");
    ASSERT_TRUE(writer->BeginKeys(to_piece(keys("key-0-a", "key-1-a", "key-2-a"))));
    ASSERT_TRUE(writer->Write(prefix + "value-0-a"));
    ASSERT_TRUE(writer->Write(prefix + "value-1-a"));
    ASSERT_TRUE(writer->EndKeys());
    ASSERT_TRUE(writer->BeginKeys(to_piece(keys("key-0-a", "key-1-a", "key-2-b"))));
    ASSERT_TRUE(writer->Write(prefix + "value-0-b"));
    ASSERT_TRUE(writer->Write(prefix + "value-1-b"));
    ASSERT_TRUE(writer->EndKeys());
}

std::set<Record> read_iterator(CacheIterator iterator) {
    std::set<Record> ret;
    while(iterator.Next()) {
        ret.insert(record(iterator.Keys(), static_cast<std::string*>(iterator.Value())));
    }
    return ret;
}


TEST(CacheIteratorTest, TestEmpty) {
    system("rm -rf test_data");
    {
        FileCacheManager manager("./test_data/", "task-1");
        CacheManager::Writer* writer = manager.GetWriter("node-1");
        ASSERT_TRUE(writer->BeginKeys(to_piece(keys("key-0-a", "key-1-a", "key-2-a"))));
        ASSERT_TRUE(writer->EndKeys());
    }

    FileCacheManager manager("./test_data/", "");
    CacheManager::Reader* reader = manager.GetReader("node-1");
    BOOST_AUTO(entity, flume::core::Entity<flume::core::Objector>::Of<flume::StringObjector>(""));
    CacheIterator iterator(reader, entity);
    ASSERT_FALSE(iterator.Next());
}

TEST(CacheIteratorTest, TestIterator) {
    system("rm -rf test_data");
    write_doodle("./test_data/", "task-1", "task-1");
    write_doodle("./test_data/", "task-2", "task-2");

    FileCacheManager manager("./test_data/", "");
    CacheManager::Reader* reader = manager.GetReader("node-1");
    BOOST_AUTO(entity, flume::core::Entity<flume::core::Objector>::Of<flume::StringObjector>(""));
    CacheIterator iterator(reader, entity);
    std::set<Record> expect;
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-a"), "task-1value-0-a"));
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-a"), "task-1value-1-a"));
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-b"), "task-1value-0-b"));
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-b"), "task-1value-1-b"));
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-a"), "task-2value-0-a"));
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-a"), "task-2value-1-a"));
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-b"), "task-2value-0-b"));
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-b"), "task-2value-1-b"));
    ASSERT_EQ(expect, read_iterator(iterator));
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
