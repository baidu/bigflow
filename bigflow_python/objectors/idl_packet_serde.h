/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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
// Author: Zhang Xiaohu <bigflow-opensource@baidu.com>
//

#ifndef BIGFLOW_PYTHON_IDL_PACKET_SERDE_H
#define BIGFLOW_PYTHON_IDL_PACKET_SERDE_H

#include "boost/python.hpp"

#include <exception>

#include "glog/logging.h"

#include "bigflow_python/objectors/python_objector.h"

namespace baidu {
namespace bigflow {
namespace python {

class IdlPacketSerde : public PythonObjector {
public:
    IdlPacketSerde() : _log_type(LOG_BIN) {}
    virtual ~IdlPacketSerde(){}
public:
    virtual void setup(const std::string& config);

    virtual uint32_t serialize(void* object, char* buffer, uint32_t buffer_size);

    virtual void* deserialize(const char* buffer, uint32_t buffer_size);

    virtual void release(void* object);

private:
    enum LOG_TYPE {
        LOG_TEXT = 0,
        LOG_PB = 1,
        LOG_BIN = 2,
    };

    // logagent protocol specification:
    // if type is 0, then the body is normal log.
    // if type is 1, then its body contains the last file name,
    // if type is 2, then its body contains specific string, like
    // "This is a heartbeat message"
    enum logagent_packet_type_t {
        LOGAGENT_TYPE_NORMAL = 0,
        LOGAGENT_TYPE_EOF = 1,
        LOGAGENT_TYPE_HB = 2,
    };

    LOG_TYPE _log_type;
};

} // namespace python
} // namespace bigflow
} // namespace baidu

#endif // BIGFLOW_PYTHON_IDL_PACKET_SERDE_H
