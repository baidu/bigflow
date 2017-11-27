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
// Author: Pan Yunhong <panyunhong@baidu.com>
//

#include "bigflow_python/objectors/tuple_serde.h"

#include <iostream>

namespace baidu {
namespace bigflow {
namespace python {

void TupleSerde::setup(const std::string& config) {
    boost::python::object real_tuple_obj = get_origin_serde(CPickleSerde().loads(config));
    boost::python::object tuple_args = real_tuple_obj.attr("get_args")();

    int64_t args_len = list_len(tuple_args);
    for (int64_t i = 0; i < args_len; ++i) {
        boost::python::object item = list_get_item(tuple_args, i);
        SerdeImplWrapper serde_element = create_serde_impl_wrapper(item);
        _tuple_element_serde.push_back(serde_element);
    }
}

uint32_t TupleSerde::serialize(void* object, char* buffer, uint32_t buffer_size) {
    boost::python::object* py_obj = static_cast<boost::python::object*>(object);
    CHECK_EQ((int64_t)_tuple_element_serde.size(), tuple_len(*py_obj));

    char* current = buffer;
    char* buffer_end = buffer + buffer_size;

    for (size_t i = 0; i < _tuple_element_serde.size(); ++i) {
        boost::python::object tuple_ele = tuple_get_item(*py_obj, i);
        char* content = current + sizeof(int32_t);
        uint32_t left = buffer_end > content ? buffer_end - content : 0;
        uint32_t elem_size = _tuple_element_serde[i].serialize(&tuple_ele, content, left);
        if (content + elem_size <= buffer_end) {
            memcpy(current, &elem_size, sizeof(uint32_t));
        }
        current = content + elem_size;
    }
    return current - buffer;
}

void* TupleSerde::deserialize(const char* buffer, uint32_t buffer_size) {
    CHECK_GT(buffer_size, 0);
    size_t tuple_size = _tuple_element_serde.size();
    boost::python::tuple* result_tuple = new
        boost::python::tuple((boost::python::handle<>(PyTuple_New(tuple_size))));

    uint32_t current_start = 0;
    for (size_t i = 0; i < tuple_size; ++i) {
        uint32_t element_size = 0;
        memcpy(&element_size, buffer+current_start, sizeof(uint32_t));

        current_start += sizeof(uint32_t);
        void* element = _tuple_element_serde[i].deserialize(buffer+current_start,
                element_size);
        tuple_set_item(*result_tuple, i, *static_cast<boost::python::object*>(element));
        current_start += element_size;
        _tuple_element_serde[i].release_object(element);
    }
    CHECK_EQ(buffer_size, current_start);
    return result_tuple;
}

void TupleSerde::release(void* object) {
    delete static_cast<boost::python::tuple*>(object);
}

} // namespace python
} // namespace bigflow
} // namespace baidu
