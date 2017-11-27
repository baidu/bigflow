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
// Author: Zhang Yuncong (zhangyuncong@baidu.com)
//
// Description: Flume Hadoop Conf

// Read Conf from specified conf file path.
//
// This action is same as the hadoop-client if we don't set the flag "--conf".

#ifndef FLUME_UTIL_HADOOP_CONF_H
#define FLUME_UTIL_HADOOP_CONF_H

#include <string>
#include <map>

namespace baidu {
namespace flume {
namespace util {

static const char kFsDefaultName[] = "fs.defaultFS";
static const char kHadoopJobUgi[] = "hadoop.job.ugi";

class HadoopConf {
public:
    typedef std::map<std::string, std::string> JobConfMap;
    HadoopConf(const std::string& conf_file_path, const JobConfMap& jobconf);
    HadoopConf(const std::string& conf_file_path);
    HadoopConf(const JobConfMap& jobconf);

    const std::string& hadoop_conf_path() const { return m_hadoop_conf_path; }

    const std::string& hadoop_job_ugi() const { return m_hadoop_job_ugi; }
    const std::string& fs_defaultfs() const { return m_fs_defaultfs; }

private:
    void load_from_map(const JobConfMap& jobconf);
    void load_from_sysenv();
    void load_from_conffile(const std::string& conf_file_path);

private:
    std::string m_hadoop_conf_path;
    std::string m_hadoop_job_ugi;
    std::string m_fs_defaultfs;
};

} // namespace util
} // namespace flume
} // namespace baidu

#endif  // FLUME_UTIL_HADOOP_CONF_H

