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
// Author: Pan Yunhong <bigflow-opensource@baidu.com>
//

#include "bigflow_python/objectors/optional_serde.h"

#include <iostream>

#include "glog/logging.h"

namespace baidu {
namespace bigflow {
namespace python {

void OptionalSerde::setup(const std::string& config) {
    using ::baidu::flume::Reflection;

    boost::python::object origin_serde_obj = get_origin_serde_obj(config);
    _serde_impl = create_serde_impl_wrapper(origin_serde_obj);
}

uint32_t OptionalSerde::serialize(void* object, char* buffer, uint32_t buffer_size) {
    boost::python::object* py_obj = static_cast<boost::python::object*>(object);
    if (*py_obj == boost::python::object()) {
        return 0;
    }
    uint32_t buffer_left_size = buffer_size;
    uint32_t result = 1;
    if (buffer_size >= 1) {
        *buffer = '1';
    }
    if (buffer_size >= 1) {
        --buffer_left_size;
    } else {
        buffer_left_size = 0;
    }

    result += _serde_impl.serialize(object, buffer + result, buffer_left_size);
    return result;
}

void* OptionalSerde::deserialize(const char* buffer, uint32_t buffer_size) {
    if (buffer_size == 0) {
        return new boost::python::object();
    }
    return _serde_impl.deserialize(buffer+1, buffer_size-1);
}

void OptionalSerde::release(void* object) {
    delete static_cast<boost::python::object*>(object);
}

} // namespace python
} // namespace bigflow
} // namespace baidu
