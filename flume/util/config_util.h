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
// Author: Wang Cong <bigflow-opensource@baidu.com>
//
// Utility for some default config parameters.

#ifndef FLUME_RUNTIME_CONFIG_UTIL_H
#define FLUME_RUNTIME_CONFIG_UTIL_H

#include <string>

namespace baidu {
namespace flume {
namespace util {

std::string DefaultHadoopConfigPath();

std::string DefaultHadoopClientPath();

std::string DefaultSparkHomePath();

std::string DefaultTempHDFSPath(const std::string& unique_id);

std::string DefaultTempLocalPath(const std::string& unique_id);

//   hdfs://host:port/xx => /xx
//   hdfs:///xx => /xx
//   /xx        => /xx
std::string RemoveHDFSPrefix(const std::string& path);

}  // namespace util
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_CONFIG_UTIL_H
