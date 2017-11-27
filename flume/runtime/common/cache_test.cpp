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
//
// Interface, write datas to a storage like file, database and so on. Corresponding to
// SINK_NODE in logical execution plan. See flume/doc/runtime.rst for details.
#include "gtest/gtest.h"

#include "flume/core/emitter.h"
#include "flume/runtime/common/cache_loader.h"
#include "flume/runtime/common/testing/cache_sinker.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace {

struct TestRecord {
    TestRecord(){}
    TestRecord(std::vector<std::string> keys, std::string value) : keys(keys), value(value) {}
    std::vector<std::string> keys;
    std::string value;
};

bool operator == (const TestRecord& left, const TestRecord& right) {
    return left.keys == right.keys && left.value == right.value;
}

}

toft::StringPiece buffer;
#define S(input) (&(buffer = input))

template<typename T>
class EmitterToVector : public core::Emitter{
public:
    virtual T transform(void* object) {
        return *static_cast<T*>(object);
    }

    virtual bool Emit(void *object) {
        m_output.push_back(transform(object));
        return true;
    }

    virtual void Done() {
    }

    const std::vector<T>& output() {
        return m_output;
    }
private:
    std::vector<T> m_output;
};

class TestEmitter : public EmitterToVector<TestRecord> {
public:
    typedef EmitterToVector<TestRecord> BaseType;
    virtual TestRecord transform(void* object) {
        CacheRecord* record = static_cast<CacheRecord*>(object);
        return TestRecord(record->keys, record->value.as_string());
    }
};

#define BEGIN_KEY(key2)\
do {\
keys[1] = key2;\
p_keys.assign(keys.begin(), keys.end());\
sinker.Open(p_keys);\
} while(0)

#define SINK_DATA(value)\
do {\
sinker.Sink(S(value));\
records.push_back(TestRecord(keys, value));\
} while(0)

TEST(TestCache, TestAll) {
    std::vector<std::string> keys;
    std::vector<toft::StringPiece> p_keys;
    std::vector<TestRecord> records;
    keys.push_back("keya1");
    keys.push_back("keyb1");

    {
        CacheSinker sinker;
        sinker.Setup("./node_1");
        BEGIN_KEY("keyb1");
        SINK_DATA("abc");
        SINK_DATA("cde");
        sinker.Close();
        BEGIN_KEY("keyb2");
        sinker.Close();
        BEGIN_KEY("keyb3");
        SINK_DATA("ghi");
        sinker.Close();
    }

    {
        TestEmitter emitter;
        CacheLoader loader;
        loader.Setup("./node_1");
        std::vector<std::string> files;
        loader.Split("./node_1", &files);
        ASSERT_EQ(1u, files.size());
        ASSERT_EQ("./node_1/0", files[0]);
        loader.Load(files[0], &emitter);
        ASSERT_EQ(records, emitter.output());
    }

}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
