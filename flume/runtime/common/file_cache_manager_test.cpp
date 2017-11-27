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
// Author: Zhang Yuncong (zhangyuncong@baidu.com)

#include "flume/runtime/common/file_cache_manager.h"

#include <cstring>
#include <iterator>
#include <list>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/string/string_piece.h"

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

Record record(std::vector<toft::StringPiece> keys, toft::StringPiece value) {
    Record ret;
    std::transform(keys.begin(), keys.end(), std::back_inserter(ret.keys), as_string);
    ret.value = value.as_string();
    return ret;
}

Record record(std::vector<std::string> keys, std::string value) {
    Record ret;
    ret.keys = keys;
    ret.value = value;
    return ret;
}

TEST(FileCacheManagerTest, TestReader) {
    {
        FileCacheManager cache_manager_to_write_1("./testdata/", "task-1");
        FileCacheManager cache_manager_to_write_2("./testdata/", "task-2");
        CacheManager::Writer* writer_1_1 = cache_manager_to_write_1.GetWriter("node-1");
        CacheManager::Writer* writer_1_2 = cache_manager_to_write_2.GetWriter("node-1");
        CacheManager::Writer* writer_2 = cache_manager_to_write_1.GetWriter("node-2");

        ASSERT_TRUE(writer_1_1->BeginKeys(to_piece(keys("key-0-a", "key-1-a", "key-2-a"))));
        ASSERT_TRUE(writer_1_1->Write("value-0-a"));
        ASSERT_TRUE(writer_1_1->Write("value-1-a"));
        ASSERT_TRUE(writer_1_1->EndKeys());

        writer_1_1 = cache_manager_to_write_1.GetWriter("node-1");
        ASSERT_TRUE(writer_1_1->BeginKeys(to_piece(keys("key-0-a", "key-1-a", "key-2-b"))));
        ASSERT_TRUE(writer_1_1->Write("value-0-b"));
        ASSERT_TRUE(writer_1_1->Write("value-1-b"));
        ASSERT_TRUE(writer_1_1->EndKeys());

        ASSERT_TRUE(writer_1_2->BeginKeys(to_piece(keys("key-0-a", "key-1-a", "key-2-c"))));
        ASSERT_TRUE(writer_1_2->Write("value-0-c"));
        ASSERT_TRUE(writer_1_2->Write("value-1-c"));
        ASSERT_TRUE(writer_1_2->EndKeys());


        ASSERT_TRUE(writer_2->BeginKeys(to_piece(keys("key-0-a", "key-1-a", "key-2-a"))));
        ASSERT_TRUE(writer_2->Write("value-0-a"));
        ASSERT_TRUE(writer_2->Write("value-1-a"));
        ASSERT_TRUE(writer_2->EndKeys());
    }

    FileCacheManager cache_manager_to_read("./testdata/", "");

    CacheManager::Reader* reader_1 = cache_manager_to_read.GetReader("node-1");
    std::vector<std::string> splits;
    reader_1->GetSplits(&splits);
    ASSERT_EQ(2u, splits.size());

    std::set<Record> records;
    CacheManager::Iterator* iterator = reader_1->Read(splits[0]);
    while(iterator->Next()) {
        records.insert(record(iterator->Keys(), iterator->Value()));
    }

    iterator = reader_1->Read(splits[1]);
    while(iterator->Next()) {
        records.insert(record(iterator->Keys(), iterator->Value()));
    }

    std::set<Record> expect;
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-a"), "value-0-a"));
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-a"), "value-1-a"));

    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-b"), "value-0-b"));
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-b"), "value-1-b"));

    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-c"), "value-0-c"));
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-c"), "value-1-c"));

    ASSERT_EQ(expect, records);

    records.clear();
    splits.clear();
    expect.clear();

    CacheManager::Reader* reader_2 = cache_manager_to_read.GetReader("node-2");
    reader_2->GetSplits(&splits);
    ASSERT_EQ(1u, splits.size());
    iterator = reader_2->Read(splits[0]);
    while(iterator->Next()) {
        records.insert(record(iterator->Keys(), iterator->Value()));
    }
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-a"), "value-0-a"));
    expect.insert(record(keys("key-0-a", "key-1-a", "key-2-a"), "value-1-a"));

    ASSERT_EQ(expect, records);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
