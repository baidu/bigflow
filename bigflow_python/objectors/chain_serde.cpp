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

#include "bigflow_python/common/python.h"

#include "bigflow_python/objectors/chain_serde.h"

#include <iostream>
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

void ChainSerde::setup(const std::string& config) {
    boost::python::object real_chain_obj = get_origin_serde(CPickleSerde().loads(config));
    boost::python::object chain_args = real_chain_obj.attr("get_args")();

    int64_t args_len = list_len(chain_args);
    LOG(INFO) << "chain serde args_len:" << args_len;
    for (int64_t i = 0; i < args_len; ++i) {
        boost::python::object item = list_get_item(chain_args, i);
        SerdeImplWrapper serde_element = create_serde_impl_wrapper(item);
        _chain_element_serde.push_back(serde_element);
    }
}

uint32_t ChainSerde::serialize(void* object, char* buffer, uint32_t buffer_size) {
    void* mid_object = object;
    void* last_object = NULL;
    uint32_t elem_size = 0;
    for (size_t i = 0; i < _chain_element_serde.size(); ++i) {
        elem_size = _chain_element_serde[i].serialize(mid_object, buffer, buffer_size);
        if (elem_size == 0 || elem_size > buffer_size) {
            return elem_size;
        }

        mid_object = new boost::python::str(buffer, elem_size);
        if (last_object != NULL) {
            delete static_cast<boost::python::object*>(last_object);
        }
        last_object = mid_object;
    }

    if (last_object != NULL) {
        delete static_cast<boost::python::object*>(last_object);
    }

    return elem_size;
};

void* ChainSerde::deserialize(const char* buffer, uint32_t buffer_size) {
    char* buff = const_cast<char*>(buffer);
    Py_ssize_t size = buffer_size;
    boost::python::object* last_object = NULL;
    for (size_t i = _chain_element_serde.size() - 1; i > 0; --i) {
        void* object = _chain_element_serde[i].deserialize(buff, size);
        boost::python::object* py_obj = static_cast<boost::python::object*>(object);
        PyString_AsStringAndSize(py_obj->ptr(), &buff, &size);
        if (last_object != NULL) {
            delete last_object;
        }
        last_object = py_obj;
    }

    return _chain_element_serde[0].deserialize(buff, size);
}

void ChainSerde::release(void* object) {
    delete static_cast<boost::python::object*>(object);
}

} // namespace python
} // namespace bigflow
} // namespace baidu
