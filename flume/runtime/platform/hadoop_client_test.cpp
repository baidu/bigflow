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
// Author: Wen Xiang <wenxiang@baidu.com>

#include "flume/runtime/platform/hadoop_client.h"

#include <cstring>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/string/algorithm.h"

#include "flume/core/key_reader.h"
#include "flume/core/logical_plan.h"
#include "flume/core/processor.h"
#include "flume/flume.h"
#include "flume/runtime/backend.h"
#include "flume/runtime/io/io_format.h"
#include "flume/runtime/resource.h"
#include "flume/runtime/testing/test_marker.h"

namespace baidu {
namespace flume {
namespace runtime {

using ::testing::_;
using ::testing::AtLeast;

TEST(HadoopClientTest, Basic) {
    std::string hadoop_home = google::StringFromEnv("HADOOP_HOME", "");
    HadoopClient client(hadoop_home + "/bin/hadoop", "", ".");
    client.Commit().WithArg("version");
}

TEST(HadoopClientTest, ReadLine) {
    TestMarker reader;
    EXPECT_CALL(reader, Mark(_)).Times(AtLeast(1));

    std::string hadoop_home = google::StringFromEnv("HADOOP_HOME", "");
    HadoopClient client(hadoop_home + "/bin/hadoop", "", ".");
    EXPECT_NE(0, client.CommitWithArgs(std::vector<std::string>(1, "fs"),  // args
                                       std::vector<std::string>(),         // classpaths
                                       std::vector<std::string>(),         // libs
                                       toft::NewPermanentClosure(&reader, &TestMarker::Mark),
                                       toft::NewPermanentClosure(&reader, &TestMarker::Mark)));
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
