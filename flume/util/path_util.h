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
// Author: Zhang Yuncong (bigflow-opensource@baidu.com)
//
// Description: Flume path util

#ifndef FLUME_UTIL_PATH_UTIL_H
#define FLUME_UTIL_PATH_UTIL_H

#include <string>

namespace baidu {
namespace flume {
namespace util {

class HadoopConf;

// translate the input_path to the toft style
// if the path is already toft style, return itself
// if the path uri of hdfs path doesn't have the domain & port,
// such as hdfs:///app/1.txt, will use environment variable "fs_defaultFS"
// the ugi will use environment variable "hadoop_job_ugi"
//
// eg.(the $p,$u is extract from the $haddop_job_ugi)
//     /hdfs/xxxx               ==>  /hdfs/xxxx
//     hdfs://x.baidu.com/x/1   ==>  /hdfs/x.baidu.com?username=$u/x/1
//     hdfs:///app/1.txt        ===> /hdfs/$fs_defaultFS?username=$p/app/1.txt
//
// user should aware the path environment variable is not set on the client side sometimes
std::string GetToftStylePath(const std::string& input_path, const HadoopConf& hadoop_conf);

// judge if the path is hdfs path
// the input path needs to be a toft style path
bool IsHdfsPath(const std::string& input_path);

// hide password of toft path
std::string HideToftPathPassword(const std::string& input_path);

} // namespace util
} // namespace flume
} // namespace baidu

#endif  // FLUME_UTIL_PATH_UTIL_H
