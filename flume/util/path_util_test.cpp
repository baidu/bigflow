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

#include "boost/filesystem.hpp"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"

#include "flume/util/path_util.h"
#include "flume/util/hadoop_conf.h"

namespace baidu {
namespace flume {
namespace util {

TEST(TestGetToftStylePath, CaseAll) {
    HadoopConf conf("./testdata/hadoop-site.xml");

    ASSERT_EQ("/hdfs/xxx", GetToftStylePath("/hdfs/xxx", conf));
    ASSERT_EQ("/hdfs/a.b.com:123?username=root,password=root/x/1",
        GetToftStylePath("hdfs://a.b.com:123/x/1", conf));
    ASSERT_EQ("/hdfs/cq01-odsp-q3zf003bak.cq01.baidu.com:34275?username=root,password=root/x/1",
        GetToftStylePath("hdfs:///x/1", conf));

    ASSERT_EQ("/abcd", GetToftStylePath("file:///abcd", conf));
    ASSERT_EQ(boost::filesystem::current_path().string() + "/abcd",
            GetToftStylePath("file://abcd", conf));

    HadoopConf conf2("./testdata/hadoop-site-invalid-path.xml");
    ASSERT_EQ(
        "/hdfs/cq01-odsp-q3zf003bak.cq01.baidu.com:34275?username=root,password=root/app/test",
        GetToftStylePath("hdfs:///app/test", conf2));

}

TEST(TestIsHdfsPath, CaseAll) {
    ASSERT_TRUE(IsHdfsPath("/hdfs/1111"));
    ASSERT_FALSE(IsHdfsPath("/local/1111"));
}

} // namespace util
} // namespace flume
} // namespace baidu

