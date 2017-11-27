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

#include "bigflow_python/functors/serde_wrapper_fn.h"

#include "boost/python.hpp"

#include "bigflow_python/common/python.h"
#include "bigflow_python/delegators/python_processor_delegator.h"
#include "bigflow_python/python_args.h"
#include "bigflow_python/python_interpreter.h"

namespace baidu {
namespace bigflow {
namespace python {

SerdeWrapperFn::SerdeWrapperFn():_is_serialize(true)
                                , _objector(NULL)
                                , _buffer(NULL)
                                , _buffer_size(0)
                                , _apply_index(-1) {}

SerdeWrapperFn::~SerdeWrapperFn() {
    if (_buffer != NULL) {
        delete []_buffer;
        _buffer = NULL;
    }
    if (_objector != NULL) {
        delete _objector;
        _objector = NULL;
    }
}

void SerdeWrapperFn::Setup(const std::string& config) {
    boost::python::object py_obj = CPickleSerde().loads(config);
    CHECK(is_tuple(py_obj)) << "Serde Wrapper config parameter must be tuple";
    CHECK(tuple_len(py_obj) >= 3);
    _is_serialize = boost::python::extract<bool>(tuple_get_item(py_obj, 0));

    boost::python::object py_objector = tuple_get_item(py_obj, 1);
    boost::python::object objector_name = py_objector.attr("get_entity_name")();
    std::string name_key = boost::python::extract<std::string>(objector_name);
    boost::python::object entity_names_dict =
            boost::python::import("bigflow.core.entity_names").attr("__dict__");
    _objector_name = boost::python::extract<std::string>(entity_names_dict[name_key]);
    _objector_config = boost::python::extract<std::string>(
            py_objector.attr("get_entity_config")());

    flume::core::Entity<flume::core::Objector> entity(_objector_name, _objector_config);
    _objector = entity.CreateAndSetup();
    _buffer = new char[INIT_BUFFER_SIZE];
    _buffer_size = INIT_BUFFER_SIZE;

    boost::python::object apply_index = tuple_get_item(py_obj, 2);
    _apply_index = boost::python::extract<int64_t>(apply_index);
}

void SerdeWrapperFn::apply_tuple_item(const boost::python::object& apply_obj,
        flume::core::Emitter* emitter) {
    CHECK(is_tuple(apply_obj) || is_list(apply_obj));
    boost::python::object item = tuple_get_item(apply_obj, _apply_index);
    if (_is_serialize) {
        uint32_t need_size = _objector->Serialize(&item, _buffer, _buffer_size);
        if (need_size > _buffer_size) {
            delete []_buffer;
            _buffer_size = need_size;
            _buffer = new char[_buffer_size];
            need_size = _objector->Serialize(&item, _buffer, _buffer_size);
        }
        boost::python::str result(_buffer, need_size);
        emitter->Emit(&result);
    } else {
        char* buf = NULL;
        Py_ssize_t size = 0;
        PyString_AsStringAndSize(item.ptr(), &buf, &size);
        void *deserialize_obj = _objector->Deserialize(buf, size);
        emitter->Emit(deserialize_obj);
        _objector->Release(deserialize_obj);
    }
}

void SerdeWrapperFn::apply_whole_object(const boost::python::object& apply_obj,
        flume::core::Emitter* emitter) {
    if (_is_serialize) {
        uint32_t need_size = _objector->Serialize((void*)&apply_obj, _buffer, _buffer_size);
        if (need_size > _buffer_size) {
            delete []_buffer;
            _buffer_size = need_size;
            _buffer = new char[_buffer_size];
            need_size = _objector->Serialize((void*)&apply_obj, _buffer, _buffer_size);
        }
        boost::python::str result(_buffer, need_size);
        emitter->Emit(&result);
    } else {
        char* buf = NULL;
        Py_ssize_t size = 0;
        PyString_AsStringAndSize(apply_obj.ptr(), &buf, &size);
        void *deserialize_obj = _objector->Deserialize(buf, size);
        emitter->Emit(deserialize_obj);
        _objector->Release(deserialize_obj);
    }
}
void SerdeWrapperFn::call(void* object, flume::core::Emitter* emitter) {
    PythonArgs* args = static_cast<PythonArgs*>(object);
    CHECK_NOTNULL(args);
    CHECK_NOTNULL(args->args);
    CHECK(_buffer != NULL) << "Buffer must be initialize before serialize";
    PyObject* raw_ptr = args->args->ptr();

    boost::python::object input_args(boost::python::handle<>(boost::python::borrowed(raw_ptr)));
    boost::python::object apply_obj = tuple_get_item(input_args, 0);
    if (_apply_index >= 0) {
        apply_tuple_item(apply_obj, emitter);
    } else {
        apply_whole_object(apply_obj, emitter);
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu


