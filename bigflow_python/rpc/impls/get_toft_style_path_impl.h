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

#include <map>
#include <string>

#include "flume/util/path_util.h"
#include "flume/util/hadoop_conf.h"

namespace baidu {
namespace bigflow {
namespace service {

void ServiceImpl::get_toft_style_path(
        ::google::protobuf::RpcController* controller,
        const GetToftStylePathRequest* request,
        SingleStringResponse* response,
        ::google::protobuf::Closure* done) {

    std::map<std::string, std::string> job_conf_map;
    std::string fs_defaultfs = request->fs_defaultfs();
    if (!fs_defaultfs.empty()) {
        job_conf_map["fs.defaultFS"] = fs_defaultfs;
        LOG(INFO) << "get hadoop fs_defaultfs = " << job_conf_map["fs.defaultFS"];
    }
    std::string job_ugi = request->hadoop_job_ugi();
    if (!job_ugi.empty()) {
        job_conf_map["hadoop.job.ugi"] = job_ugi;
        LOG(INFO) << "get hadoop job ugi = " << job_conf_map["hadoop.job.ugi"];
    }

    baidu::flume::util::HadoopConf conf(request->hadoop_conf_path(), job_conf_map);
    std::string result = baidu::flume::util::GetToftStylePath(request->input_path(), conf);

    ResponseStatus* status = response->mutable_status();
    status->set_success(true);
    response->set_response(result);

    if (done) {
        // Finish this request so that response will be send
        // back to client ASAP
        done->Run();
    }
}

}  // namespace service
}  // namespace bigflow
}  // namespace baidu

