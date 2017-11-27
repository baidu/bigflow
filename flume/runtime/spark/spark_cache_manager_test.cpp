/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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

//
// Created by zhangyuncong on 2017/9/22.
// Author: Zhang Yuncong (bigflow-opensource@baidu.com)

#include "flume/runtime/spark/spark_cache_manager.h"

#include <map>
#include <string>
#include <vector>
#include <algorithm>

#include "thirdparty/boost/lexical_cast.hpp"
#include "thirdparty/gtest/gtest.h"
#include "thirdparty/gmock/gmock.h"

#include "toft/crypto/uuid/uuid.h"
#include "toft/base/string/string_piece.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/util/iteration.h"
#include "flume/runtime/spark/shuffle_protocol.h"


class CacheInputExecutorTest;

class CacheInputExecutorTest;
namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Field;
using ::testing::Return;
using ::testing::InSequence;
using ::testing::ElementsAre;

class SparkCacheManagerTest : public ::testing::Test {
public:
    virtual void SetUp() {
    }
};

class Listener {
public:
    void on_data(const toft::StringPiece& k, const toft::StringPiece& v) {
        std::string key = k.as_string();
        ShuffleHeader* header = ShuffleHeader::cast(&*key.begin());
        const char* content = header->content();

        on_data_come(header->task_index(), std::string(content, k.size() - sizeof(ShuffleHeader)), v.as_string());
    }

    MOCK_METHOD3(on_data_come, void (uint32_t, const std::string&, const std::string&));
};

std::vector<toft::StringPiece> tmp_keys(const std::initializer_list<std::string>& params) {
    std::vector<toft::StringPiece> ret(params.size());
    for(auto param : enumerate(params)) {
        ret[param.get<0>()].set(param.get<1>());
    }
    return ret;
}

inline char* string_as_buf(std::string& s) {
    return &*s.begin();
}

std::string size_and_key(const std::string& key) {
    uint32_t len = htonl(key.size());
    std::string ret(key.size() + sizeof(uint32_t), '\0');
    char* buf = string_as_buf(ret);
    memcpy(buf, &len, sizeof(uint32_t));
    memcpy(buf + sizeof(uint32_t), key.data(), key.size());
    return ret;
}

TEST_F(SparkCacheManagerTest, TestAll) {
        PbSparkJob::PbSparkJobInfo job_message;
        for (size_t i = 0; i != 3; ++i) {
            job_message.add_cache_node_id(boost::lexical_cast<std::string>(i));
            job_message.add_cache_task_id(i);
        }
        Listener listener;
        auto emitter = std::bind(&Listener::on_data, &listener, std::placeholders::_1, std::placeholders::_2);
        SparkCacheManager cache_manager(emitter, job_message);

        ShuffleHeader header;

        auto writer1 = cache_manager.GetWriter("1");
        auto writer2 = cache_manager.GetWriter("2");

        {
            InSequence in_sequence;
            EXPECT_CALL(listener, on_data_come(1, size_and_key("g") + size_and_key("key1") + "2", "value1"));
            EXPECT_CALL(listener, on_data_come(1, size_and_key("g") + size_and_key("key1") + "2", "value2"));
            EXPECT_CALL(listener, on_data_come(2, size_and_key("g") + "1", "value1"));
            EXPECT_CALL(listener, on_data_come(2, size_and_key("g") + "1", "value2"));
            EXPECT_CALL(listener, on_data_come(1, "g", "value1"));
        }

        writer1->BeginKeys(tmp_keys({"g", "key1", "2"}));
        writer1->Write("value1");
        writer1->Write("value2");
        writer1->EndKeys();

        writer2->BeginKeys(tmp_keys({"g", "1"}));
        writer2->Write("value1");
        writer2->Write("value2");
        writer2->EndKeys();

        writer1->BeginKeys(tmp_keys({"g"}));
        writer1->Write("value1");
        writer1->EndKeys();

}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
