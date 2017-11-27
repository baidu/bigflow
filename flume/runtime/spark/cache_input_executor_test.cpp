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
// Author: Zhang Yuncong (bigflow-opensource@baidu.com)

#include "flume/runtime/spark/cache_input_executor.h"

#include <map>
#include <string>
#include <vector>

#include "thirdparty/gmock/gmock.h"
#include "thirdparty/gtest/gtest.h"
#include "toft/base/string/string_piece.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/entity.h"
#include "flume/core/testing/mock_loader.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/memory_dataset.h"
#include "flume/runtime/executor.h"
#include "flume/planner/common/cache_util.h"


namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

using core::Entity;
using core::Loader;

using ::testing::_;
using ::testing::DoAll;
using ::testing::Field;
using ::testing::Return;
using ::testing::InSequence;
using ::testing::ElementsAre;

class CacheInputExecutorTest : public ::testing::Test {
public:
    virtual void SetUp() {
    }
};

class Listener {
public:
    void on_input_come(const std::vector<toft::StringPiece>&, void* obj, const toft::StringPiece&) {
        planner::CacheRecord* record = static_cast<planner::CacheRecord*>(obj);
        on_value_come(record->keys, record->content.as_string(), record->empty);
    }
    MOCK_METHOD3(on_value_come, void (const std::vector<std::string>&, const std::string&, bool empty));
    MOCK_METHOD0(on_input_done, void ());
};

std::string key(const std::string& k) {
    return std::string(3, ' ') + k;
}

TEST_F(CacheInputExecutorTest, TestKeyNumberZero) {
    PbSparkTask::PbCacheInput pb_cache_input_executor;
    pb_cache_input_executor.set_id("test");
    pb_cache_input_executor.set_cache_node_id("cache");
    pb_cache_input_executor.set_key_num(0); // Effective key num is 0. Only have global key.
    CacheInputExecutor executor(pb_cache_input_executor);
    PbExecutor pb_executor;
    std::vector<Executor*> childs;
    MemoryDatasetManager dataset_manager;
    executor.initialize(pb_executor, childs, &dataset_manager);
    std::map<std::string, Source*> sources;
    Executor& e = executor;
    e.Setup(sources);
    Source* source = e.GetSource("test", 0);
    Listener listener;
    source->RequireStream(Source::REQUIRE_OBJECT,
                          toft::NewPermanentClosure(&listener, &Listener::on_input_come),
                          toft::NewPermanentClosure(&listener, &Listener::on_input_done)
    );

    std::string v1("value1");
    std::string v2("value2");
    std::string v3("value3");

    {
        InSequence in_sequence;
        EXPECT_CALL(listener, on_value_come(ElementsAre(""), v1, false));
        EXPECT_CALL(listener, on_value_come(ElementsAre("global_key"), v2, false));
        EXPECT_CALL(listener, on_value_come(ElementsAre("global_key"), v3, false));
        EXPECT_CALL(listener, on_input_done());
    }

    e.BeginGroup("g");
    e.FinishGroup();

    executor.process_input(key(""), v1); // test when global key is empty
    executor.process_input(key("global_key"), v2);
    executor.process_input(key("global_key"), v3);
    executor.input_done();
}

std::string key(const std::string& k1, const std::string& k2) {
    std::string ret(3, ' ');
    uint32_t l1 = htonl(k1.size()); // save as network endian order.

    ret += std::string(reinterpret_cast<char*>(&l1), 4);
    ret += k1;
    ret += k2;
    return ret;
}

TEST_F(CacheInputExecutorTest, TestKeyNumberTwo) {
    PbSparkTask::PbCacheInput pb_cache_input_executor;
    pb_cache_input_executor.set_id("test");
    pb_cache_input_executor.set_cache_node_id("cache");
    pb_cache_input_executor.set_key_num(1); // effective key num(not including global key)
    CacheInputExecutor executor(pb_cache_input_executor);
    PbExecutor pb_executor;
    std::vector<Executor*> childs;
    MemoryDatasetManager dataset_manager;
    executor.initialize(pb_executor, childs, &dataset_manager);
    std::map<std::string, Source*> sources;
    Executor& e = executor;
    e.Setup(sources);
    Source* source = e.GetSource("test", 0);
    Listener listener;
    source->RequireStream(Source::REQUIRE_OBJECT,
                          toft::NewPermanentClosure(&listener, &Listener::on_input_come),
                          toft::NewPermanentClosure(&listener, &Listener::on_input_done)
    );

    std::string v1("value1");
    std::string v2("value2");

    {
        InSequence in_sequence;
        EXPECT_CALL(listener, on_value_come(ElementsAre("k1", "k2"), v1, false));
        EXPECT_CALL(listener, on_value_come(ElementsAre("k3", "k4"), v2, false));
        EXPECT_CALL(listener, on_input_done());
    }

    e.BeginGroup("g");
    e.FinishGroup();

    executor.process_input(key("k1", "k2"), v1);
    executor.process_input(key("k3", "k4"), v2);
    executor.input_done();
}



}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
