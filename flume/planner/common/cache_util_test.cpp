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

#include "flume/planner/common/cache_util.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace baidu {
namespace flume {
namespace planner {

class CacheUtilTest : public ::testing::Test {
protected:
    virtual void SetUp() { }
    virtual void TearDown() { }

private:
};

TEST_F(CacheUtilTest, Test) {
    CacheRecordObjector objector;
    CacheRecord cache_record;
    cache_record.keys.push_back("key1");
    cache_record.keys.push_back("key2");
    cache_record.content = std::string("value");
    cache_record.empty = false;

    char buf[256];
    uint32_t offset = objector.Serialize(&cache_record, buf, 256);
    CacheRecord* des = static_cast<CacheRecord*>(objector.Deserialize(buf, offset));
    std::string str(des->content.data(), des->content.size());
    EXPECT_STREQ(str.c_str(), "value");
    objector.Release(des);
}

} // namespace planner
} // namespace flume
} // namespace baidu

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
