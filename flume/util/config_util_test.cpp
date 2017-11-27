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
// Author: Zhang Yuncong <bigflow-opensource@baidu.com>
//

#include <dlfcn.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"

#include "flume/util/config_util.h"

namespace baidu {
namespace flume {
namespace util {

TEST(ConfigUtilTest, TestRemoveHDFSPrefix) {
    ASSERT_EQ("/xx", RemoveHDFSPrefix("hdfs://host:port/xx"));
    ASSERT_EQ("/xx", RemoveHDFSPrefix("hdfs:///xx"));
    ASSERT_EQ("/xx", RemoveHDFSPrefix("/xx"));
}

}  // namespace util
}  // namespace flume
}  // namespace baidu
