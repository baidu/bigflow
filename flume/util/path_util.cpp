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

#include "flume/util/path_util.h"

#include <cstddef>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "boost/filesystem.hpp"
#include "boost/regex.hpp"
#include "glog/logging.h"

#include "toft/base/uncopyable.h"
#include "toft/base/scoped_array.h"
#include "toft/base/string/string_piece.h"
#include "toft/storage/file/uri_utils.h"

#include "flume/util/hadoop_conf.h"

namespace baidu {
namespace flume {
namespace util {

std::string GetToftStylePath(const std::string& input_path, const HadoopConf& hadoop_conf) {
    LOG(INFO) << "path : " << input_path;

    // Be carefull if you want to assign some temporary string to path (which is a StringPiece).
    toft::StringPiece path(input_path);

    const std::string hdfs_prefix = "hdfs://";
    const std::string file_prefix = "file://";
    const bool is_hdfs = path.starts_with(toft::StringPiece(hdfs_prefix));
    if (is_hdfs) {
        path = path.substr(hdfs_prefix.size() - 1);
        CHECK_GE(path.size(), 2) << "path:" << input_path;
        std::string fs_name;
        int pos = path.find_first_of('/', 1);
        if (pos == 1) { // means use default hdfs
            fs_name = hadoop_conf.fs_defaultfs();
            if (toft::StringPiece(fs_name).starts_with(hdfs_prefix) && is_hdfs) {
                fs_name = fs_name.substr(hdfs_prefix.size());
            } else {
                LOG(INFO) << "input path  schema is different with hadoop conf fs name schema. "
                    << "input path: " << input_path << "; "
                    << " fs name: " << fs_name;
            }
        } else {
            CHECK_NE(std::string::npos, pos);
            fs_name = path.substr(1, pos - 1).as_string();
        }
        size_t last_pos = fs_name.find_last_not_of('/');
        if (last_pos != fs_name.size() - 1) {
            fs_name = fs_name.substr(0, last_pos + 1);
        }
        LOG(INFO) << "fs_name : " << fs_name;

        std::string ugi = hadoop_conf.hadoop_job_ugi();
        std::vector<std::string> ugis;
        CHECK(toft::UriUtils::Explode(ugi, ',', &ugis)) << "explode failed";
        CHECK_EQ(ugis.size(), 1) << "num of ugi wrong";

        const std::string& username = ugis[0];

        std::string uri_head = "/hdfs/" + fs_name +
            "?username=" + username;
        std::string rest_uri;
        CHECK(toft::UriUtils::Shift(path.as_string(), &rest_uri, 1, '/')) << "shift failed";
        return uri_head + rest_uri;
    } else {
        if (path.starts_with(file_prefix)) path = path.substr(file_prefix.size());
        CHECK(!path.empty());
        if (path[0] != '/') {
            return (boost::filesystem::current_path() / path.as_string()).string();
        }
        return path.as_string();
    }
    return input_path;
}

bool IsHdfsPath(const std::string& input_path) {
    toft::StringPiece path(input_path);
    return path.starts_with("/hdfs/");
}

std::string HideToftPathPassword(const std::string& input_path) {
    return boost::regex_replace(input_path, boost::regex("password=(.*?)/"), "password=*****/");
}

} // namespace util
} // namespace flume
} // namespace baidu
