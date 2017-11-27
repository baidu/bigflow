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
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#include <fstream>
#include <sstream>
#include <string>

#include "glog/logging.h"
#include "flume/core/entity.h"

#include "flume/proto/entity.pb.h"

namespace baidu {
namespace flume {
namespace core {

PbEntity EntityBase::ToProtoMessage() const {
    PbEntity message;
    message.set_name(m_name);
    message.set_config(m_config);
    return message;
}

void EntityBase::FromProtoMessage(const PbEntity& message) {
    m_name = message.name();
    if (message.has_config_file()) {
        std::string file = message.config_file();
        LOG(INFO) << "Start reading file: " << file.c_str();
        std::ifstream in(file.c_str(), std::ios_base::in | std::ios_base::binary);
        std::ostringstream ostream;
        ostream << in.rdbuf();
        CHECK(in) << "Read file: " << file.c_str() << " error";
        m_config = std::string(ostream.str());
        LOG(INFO) << "End reading file: " << file.c_str();
    } else {
        m_config = message.config();
    }
}

}  // namespace core
}  // namespace flume
}  // namespace baidu
