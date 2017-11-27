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

#include "bigflow_python/objectors/str_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

uint32_t StrSerde::serialize(void* object, char* buffer, uint32_t buffer_size) {
    boost::python::object* py_obj = static_cast<boost::python::object*>(object);
    char* buf = NULL;
    Py_ssize_t size = 0;
    PyString_AsStringAndSize(py_obj->ptr(), &buf, &size);
    if (size <= buffer_size) {
        memcpy(buffer, buf, size);
    }
    return size;
}

void* StrSerde::deserialize(const char* buffer, uint32_t buffer_size) {
    return new boost::python::str(buffer, buffer_size);
}

void StrSerde::release(void* object) {
    delete static_cast<boost::python::object*>(object);
}

} // namespace python
} // namespace bigflow
} // namespace baidu
