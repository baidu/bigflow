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

#include "config_util.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "toft/base/string/string_piece.h"

DEFINE_string(flume_hadoop_client, "", "path to hadoop command which are used to launch dce job.");
DEFINE_string(flume_hadoop_config, "", "path to hadoop config which are used to launch dce job.");

namespace baidu {
namespace flume {
namespace util {

std::string DefaultHadoopConfigPath() {
    if (!FLAGS_flume_hadoop_config.empty()) {
        return FLAGS_flume_hadoop_config;
    }

    std::string path = google::StringFromEnv("HADOOP_CONF_PATH", "");
    if (!path.empty()) {
        return path;
    }

    return google::StringFromEnv("HADOOP_HOME", "") + std::string("/etc/hadoop/core-site.xml");
}

std::string DefaultHadoopClientPath() {
    if (!FLAGS_flume_hadoop_client.empty()) {
        return FLAGS_flume_hadoop_client;
    }

    return google::StringFromEnv("HADOOP_HOME", "") + std::string("/bin/hadoop");
}

std::string DefaultSparkHomePath() {
    // TODO(wangcong09) Parse from GFlags maybe
    return google::StringFromEnv("SPARK_HOME", "");
}

std::string DefaultTempHDFSPath(const std::string& unique_id) {
    return "hdfs:///flume/app/tmp/" + unique_id;
}

std::string DefaultTempLocalPath(const std::string& unique_id) {
    return "./.tmp/" + unique_id;
}

//   hdfs://host:port/xx => /xx
//   hdfs:///xx => /xx
//   /xx        => /xx
std::string RemoveHDFSPrefix(const std::string& path) {
    const std::string hdfs_prefix = "hdfs://";
    const bool is_hdfs = toft::StringPiece(path).starts_with(hdfs_prefix);
    if (is_hdfs) {
        std::string ret = path.substr(hdfs_prefix.size());
        std::string::size_type pos = ret.find('/');
        CHECK_NE(std::string::npos, pos);
        return ret.substr(pos);
    } else {
        return path;
    }
}

}  // namespace util
}  // namespace flume
}  // namespace baidu
