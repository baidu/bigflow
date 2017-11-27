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

#include "bigflow_python/objectors/set_serde.h"

#include <iostream>

namespace baidu {
namespace bigflow {
namespace python {

void SetSerde::setup(const std::string& config) {
    boost::python::object set_obj = get_origin_serde(CPickleSerde().loads(config));
    boost::python::object value_serde = set_obj.attr("get_value_serde")();

    std::string reflect_str = get_entity_reflect_str(value_serde);
    std::string serde_config = get_entity_config_str(value_serde);
    flume::core::Entity<flume::core::Objector> entity(reflect_str, serde_config);
    _serde.reset(entity.CreateAndSetup());
    //_serde = entity.CreateAndSetup();
    //CHECK(_serde != NULL);
}

uint32_t SetSerde::serialize(void* object, char* buffer, uint32_t buffer_size) {
    boost::python::object* set_obj = static_cast<boost::python::object*>(object);
    PyObject *it = PyObject_GetIter(set_obj->ptr());

    PyObject *value = NULL;
    uint32_t buffer_left_size = buffer_size;
    uint32_t result = 0;

    while ((value = PyIter_Next(it)) != NULL) {
        boost::python::object item =
                    boost::python::object(boost::python::handle<>(value));
        char *element_start = buffer + result;
        char *content_start = buffer + result + sizeof(uint32_t);
        bool copy_size = false;
        if (buffer_left_size >= sizeof(uint32_t)) {
            buffer_left_size -= sizeof(uint32_t);
            copy_size = true;
        } else {
            buffer_left_size = 0;
        }

        uint32_t element_size = \
            _serde->Serialize((void*)(&item), content_start, buffer_left_size);
        if (copy_size) {
            memcpy(element_start, &element_size, sizeof(element_size));
        }
        result += sizeof(uint32_t) + element_size;
        if (result <= buffer_size) {
            buffer_left_size = buffer_size - result;
        } else {
            buffer_left_size = 0;
        }
    }
    return result;
}

void* SetSerde::deserialize(const char* buffer, uint32_t buffer_size) {
    uint32_t buffer_offset = 0;
    boost::python::object* result_set = new
        boost::python::object((boost::python::handle<>(PySet_New(NULL))));

    while (buffer_offset < buffer_size) {
        uint32_t left = buffer_size - buffer_offset;
        CHECK_GE(left, sizeof(uint32_t)) << "Invalid left buffer size" \
            << buffer_offset << "," << buffer_size;
        uint32_t element_size = 0;
        memcpy(&element_size, buffer + buffer_offset, sizeof(uint32_t));
        CHECK_LE(element_size + sizeof(uint32_t), left);

        void* element = _serde->Deserialize(\
            buffer + buffer_offset + sizeof(uint32_t), element_size);
        PySet_Add(result_set->ptr(), static_cast<boost::python::object*>(element)->ptr());
        _serde->Release(element);
        buffer_offset += sizeof(uint32_t);
        buffer_offset += element_size;
    }
    return result_set;
}
void SetSerde::release(void* object) {
    delete static_cast<boost::python::object*>(object);
}

} // namespace python
} // namespace bigflow
} // namespace baidu
