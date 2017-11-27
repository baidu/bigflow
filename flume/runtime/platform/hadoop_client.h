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

#ifndef FLUME_RUNTIME_DCE_HADOOP_CLIENT_H
#define FLUME_RUNTIME_DCE_HADOOP_CLIENT_H

#include <string>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "glog/logging.h"
#include "toft/base/closure.h"
#include "toft/base/scoped_ptr.h"
#include "toft/system/process/sub_process.h"
#include "toft/system/threading/thread.h"

namespace baidu {
namespace flume {
namespace runtime {

class HadoopClient {
public:
    class Committer {
    public:
        explicit Committer(HadoopClient* client) : m_client(client) {}

        ~Committer() {
            if (!m_args.empty()) {
                int ret = m_client->CommitWithArgs(m_args, m_class_paths, m_ld_lib_paths,
                                                   NULL, NULL);
                LOG_IF(WARNING, ret != 0) << "Hadoop client returns: " << ret;
            }
        }

        template<typename T>
        Committer& WithArg(T arg) {
            m_args.push_back(boost::lexical_cast<std::string>(arg));
            return *this;
        }

        template<typename T1, typename T2>
        Committer& WithArg(T1 arg1, T2 arg2) {
            m_args.push_back(boost::lexical_cast<std::string>(arg1));
            m_args.push_back(boost::lexical_cast<std::string>(arg2));
            return *this;
        }

        template<typename T>
        Committer& WithClassPath(T arg) {
            m_class_paths.push_back(boost::lexical_cast<std::string>(arg));
            return *this;
        }

        template<typename T>
        Committer& WithLdLibararyPath(T arg) {
            m_ld_lib_paths.push_back(boost::lexical_cast<std::string>(arg));
            return *this;
        }

    private:
        HadoopClient* m_client;
        std::vector<std::string> m_args;
        std::vector<std::string> m_class_paths;
        std::vector<std::string> m_ld_lib_paths;
    };

public:
    explicit HadoopClient(const std::string& hadoop_client_path,
                          const std::string& hadoop_conf_path,
                          const std::string& working_dir);

    virtual ~HadoopClient();

    Committer Commit() { return Committer(this); }

    virtual int CommitWithArgs(const std::vector<std::string>& args,
                               const std::vector<std::string>& class_paths,
                               const std::vector<std::string>& ld_lib_paths,
                               toft::Closure<void (const std::string&)>* stdout_reader,
                               toft::Closure<void (const std::string&)>* stderr_reader);

private:
    std::string m_client_path;
    std::string m_conf_path;
    std::string m_working_dir;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_DCE_HADOOP_CLIENT_H
