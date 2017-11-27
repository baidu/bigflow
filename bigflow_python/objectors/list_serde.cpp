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

#include "bigflow_python/objectors/list_serde.h"
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

void SameTypeListSerde::setup(const std::string& config) {
    flume::core::Objector* element_serde = NULL;
    boost::python::object item = list_get_item(_list_schema, 0);
    _serde_impl = create_serde_impl_wrapper(item);
}

uint32_t SameTypeListSerde::serialize(void* object, char* buffer, uint32_t buffer_size) {
    boost::python::object* py_obj = static_cast<boost::python::object*>(object);

    uint32_t result = 0;
    uint32_t buffer_left_size = buffer_size;
    int64_t list_fields_len = list_len(*py_obj);

    for (int64_t i = 0; i < list_fields_len; ++i) {
        boost::python::object list_element = list_get_item(*py_obj, i);

        char *element_start = buffer + result;
        char *content_start = buffer + result + sizeof(uint32_t);
        bool copy_size = false;
        if (buffer_left_size >= sizeof(uint32_t)) {
            buffer_left_size -= sizeof(uint32_t);
            copy_size = true;
        } else {
            buffer_left_size = 0;
        }
        uint32_t element_size = _serde_impl.serialize(&list_element, \
            content_start, buffer_left_size);

        if (copy_size) {
            memcpy(element_start, &element_size, sizeof(element_size));
        }

        result += sizeof(uint32_t) + element_size;
        if (buffer_size >= result) {
            buffer_left_size = buffer_size - result;
        } else {
            buffer_left_size = 0;
        }
    }
    return result;
}

void* SameTypeListSerde::deserialize(const char* buffer, uint32_t buffer_size) {
    boost::python::object* result_list = new boost::python::list;

    uint32_t current_start = 0;
    int index = 0;
    while (current_start < buffer_size) {
        uint32_t element_size = *reinterpret_cast<const uint32_t*>(buffer + current_start);
        current_start += sizeof(uint32_t);
        void* element = _serde_impl.deserialize(buffer + current_start, element_size);
        list_append_item(*result_list, *static_cast<boost::python::object*>(element));
        current_start += element_size;
        _serde_impl.release_object(element);
    }
    CHECK_EQ(buffer_size, current_start);
    return result_list;
}

void SameTypeListSerde::release(void* object) {
    delete static_cast<boost::python::object*>(object);
}

void TupleLikeListSerde::setup(const std::string& config) {
    int64_t list_fields_len = list_len(_list_schema);

    for (int64_t i = 0; i < list_fields_len; ++i) {
        boost::python::object item = list_get_item(_list_schema, i);
        SerdeImplWrapper impl_wrapper = create_serde_impl_wrapper(item);
        _serde_impls.push_back(impl_wrapper);
    }
}

uint32_t TupleLikeListSerde::serialize(void* object, char* buffer, uint32_t buffer_size) {
    boost::python::object* py_obj = static_cast<boost::python::object*>(object);

    CHECK((int64_t)_serde_impls.size() == list_len(*py_obj));
    uint32_t result = 0;
    uint32_t buffer_left_size = buffer_size;

    for (size_t i = 0; i < _serde_impls.size(); ++i) {
        boost::python::object list_element = list_get_item(*py_obj, i);

        char *element_start = buffer + result;
        char *content_start = buffer + result + sizeof(uint32_t);
        bool copy_size = false;
        if (buffer_left_size >= sizeof(uint32_t)) {
            buffer_left_size -= sizeof(uint32_t);
            copy_size = true;
        } else {
            buffer_left_size = 0;
        }
        uint32_t element_size = _serde_impls[i].serialize(&list_element, \
                content_start, buffer_left_size);
        if (copy_size) {
            memcpy(element_start, &element_size, sizeof(element_size));
        }

        result += sizeof(uint32_t) + element_size;
        if (buffer_size >= result) {
            buffer_left_size = buffer_size - result;
        } else {
            buffer_left_size = 0;
        }
    }
    return result;
}

void* TupleLikeListSerde::deserialize(const char* buffer, uint32_t buffer_size){
    size_t list_size = _serde_impls.size();
    boost::python::object* result_list = new
        boost::python::object((boost::python::handle<>(PyList_New(list_size))));

    uint32_t current_start = 0;
    for (size_t i = 0; i < list_size; ++i) {
        uint32_t element_size = 0;
        memcpy(&element_size, buffer + current_start, sizeof(uint32_t));

        current_start += sizeof(uint32_t);
        void* element = _serde_impls[i].deserialize(buffer + current_start,
                element_size);
        list_set_item(*result_list, i, *static_cast<boost::python::object*>(element));
        current_start += element_size;
        _serde_impls[i].release_object(element);
    }
    return result_list;
}

void ListSerde::setup(const std::string& config) {
    using flume::Reflection;

    boost::python::object origin_obj = get_origin_serde(CPickleSerde().loads(config));
    boost::python::object list_fields = origin_obj.attr("get_fields")();
    int64_t fields_len = list_len(list_fields);

    if (fields_len == 1) {
        _delegate_objector = new SameTypeListSerde(list_fields);
    } else {
        _delegate_objector = new TupleLikeListSerde(list_fields);
    }
    _delegate_objector->setup(config);
}

} // namespace python
} // namespace bigflow
} // namespace baidu
