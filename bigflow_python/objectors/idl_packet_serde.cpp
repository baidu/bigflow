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

#include "bigflow_python/objectors/idl_packet_serde.h"
#include "bigflow_python/objectors/packet.idl.h"

#include <iostream>
#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

void IdlPacketSerde::setup(const std::string& config) {
    boost::python::object idl_obj = get_origin_serde(CPickleSerde().loads(config));
    boost::python::object log_type_obj = idl_obj.attr("get_log_type")();
    std::string log_type = boost::python::extract<std::string>(log_type_obj);
    if (log_type == "log_text") {
        _log_type = LOG_TEXT;
    } else if (log_type == "log_pb") {
        _log_type = LOG_PB;
    } else if (log_type == "log_bin") {
        _log_type = LOG_BIN;
    } else {
        LOG(FATAL) << "log type not support:" << log_type;
    }
    LOG(INFO) << "idl log type: " << _log_type;
}

uint32_t IdlPacketSerde::serialize(void* object, char* buffer, uint32_t buffer_size) {
    boost::python::object* py_obj = static_cast<boost::python::object*>(object);
    char* buff = NULL;
    int64_t size = 0;
    PyString_AsStringAndSize(py_obj->ptr(), &buff, &size);

    bsl::syspool pool;
    idl::packet packet_idl(&pool);
    compack::autobuffer::buffer outbuf;
    packet_idl.clear();
    packet_idl.set_type(0);
    packet_idl.set_flag(0);
    packet_idl.set_stime(time(NULL));
    packet_idl.set_body_type(_log_type);

    if (_log_type == LOG_TEXT) {
        packet_idl.set_ptr_body(buff, size);
    } else {
        // LOG_PB or LOG_BIN
        packet_idl.set_ptr_bin_body(buff, size);
    }

    size_t packet_size = 0;
    try {
        packet_size = packet_idl.save(&outbuf);
    } catch (std::exception& e) {
        LOG(FATAL) << "packet_idl.save() throw an exception:" << e.what();
    } catch (...) {
        LOG(FATAL) << "packet_idl.save() throw an unknown exception";
    }

    if (packet_size <= buffer_size) {
        memcpy(buffer, outbuf.buffer(), packet_size);
    }

    return packet_size;
}

void* IdlPacketSerde::deserialize(const char* buffer, uint32_t buffer_size) {
    bsl::syspool pool;
    idl::packet packet_idl(&pool);
    compack::Buffer idl_buf(const_cast<char*>(buffer), buffer_size);
    try {
        packet_idl.load(idl_buf);
    } catch (std::exception& e) {
        LOG(FATAL) << "packet load throw an exception " << e.what();
    } catch (...) {
        LOG(FATAL) << "packet load throw an unknow exception";
    }

    if (packet_idl.type() != LOGAGENT_TYPE_NORMAL) {
        LOG(WARNING) << "packet type is not normal, type: " << packet_idl.type();
        return new boost::python::object();
    }

    char* body_ptr = NULL;
    uint32_t body_len = 0;
    if (_log_type == LOG_TEXT) {
        body_ptr = const_cast<char*>(packet_idl.body(&body_len));
    } else {
        // LOG_PB or LOG_BIN
        body_ptr = const_cast<char*>(static_cast<const char*>(packet_idl.bin_body(&body_len)));
    }

    return new boost::python::str(body_ptr, body_len);
}

void IdlPacketSerde::release(void* object) {
    delete static_cast<boost::python::object*>(object);
}

} // namespace python
} // namespace bigflow
} // namespace baidu

