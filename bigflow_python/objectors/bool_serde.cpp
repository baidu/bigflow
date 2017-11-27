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

#include "bigflow_python/common/python.h"

#include <iostream>

#include "bigflow_python/objectors/bool_serde.h"

#include "bigflow_python/common/comparable.h"

namespace baidu {
namespace bigflow {
namespace python {

uint32_t BoolSerde::serialize(void* object, char* buffer, uint32_t buffer_size) {
    boost::python::object* py_obj = static_cast<boost::python::object*>(object);
    CHECK(is_python_bool(*py_obj));
    bool b = boost::python::extract<bool>(*py_obj);
    return AppendOrdered(b, buffer, buffer_size);
}

void* BoolSerde::deserialize(const char* buffer, uint32_t buffer_size) {
    CHECK_EQ(buffer_size, sizeof(bool));
    bool n = false;
    memcpy((char *)&n, buffer, buffer_size);
    return new boost::python::object(n);
}

void BoolSerde::release(void* object) {
    delete static_cast<boost::python::object*>(object);
}

} // namespace python
} // namespace bigflow
} // namespace baidu
