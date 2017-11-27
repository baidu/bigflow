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

#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <cstdlib>
#include <iostream>  // NOLINT
#include <vector>

#include "boost/lexical_cast.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "toft/base/array_size.h"
#include "toft/base/string/algorithm.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/encoding/base64.h"
#include "toft/encoding/shell.h"
#include "toft/system/process/sub_process.h"
#include "toft/system/process/this_process.h"

#include "flume/util/process_launcher.h"

namespace baidu {
namespace flume {
namespace runtime {

HadoopClient::HadoopClient(const std::string& hadoop_client_path,
                           const std::string& hadoop_conf_path,
                           const std::string& working_dir)
        : m_client_path(hadoop_client_path),
          m_conf_path(hadoop_conf_path),
          m_working_dir(working_dir) {}

HadoopClient::~HadoopClient() {}

int HadoopClient::CommitWithArgs(const std::vector<std::string>& args,
                                 const std::vector<std::string>& class_paths,
                                 const std::vector<std::string>& ld_lib_paths,
                                 toft::Closure<void (const std::string&)>* stdout_reader,
                                 toft::Closure<void (const std::string&)>* stderr_reader) {
    CHECK(!args.empty());
    std::vector<std::string> argv(1, m_client_path);
    argv.push_back(args[0]);
    if (!m_conf_path.empty()) {
        argv.push_back("-conf");
        argv.push_back(m_conf_path);
    }
    std::copy(args.begin() + 1, args.end(), std::back_inserter(argv));
    LOG(INFO) << "Hadoop command: " << toft::JoinCommandLine(argv);

    toft::SubProcess::CreateOptions options;

    // set ENV: HADOOP_CLASSPATH
    std::string hadoop_cpaths = toft::JoinStrings(class_paths, ":");
    if (hadoop_cpaths.size() > 0) {
        options.AddEnvironment("HADOOP_CLASSPATH", hadoop_cpaths.c_str());
    }

    // set ENV: CUSTOMIZE_LIBRARY_PATH
    std::string custom_lpaths = toft::JoinStrings(ld_lib_paths, ":");
    if (custom_lpaths.size() > 0) {
        options.AddEnvironment("CUSTOMIZE_LIBRARY_PATH", custom_lpaths.c_str());
    }

    // note: every hadoop client command should be invoked in 'resource' working dir
    options.SetWorkDirectory(m_working_dir);

    return util::ProcessLauncher().RunAndWaitExit(argv,
                                                  options,
                                                  "Hadoop client",
                                                  stdout_reader,
                                                  stderr_reader);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
