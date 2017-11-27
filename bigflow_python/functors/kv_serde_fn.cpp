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
/**
* @created:     2016/03/10
* @filename:    kv_serde_fn.cpp
* @author:      wenchunyang (bigflow-opensource@baidu.com)
*/

#include "boost/python.hpp"

#include "bigflow_python/functors/kv_serde_fn.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/delegators/python_processor_delegator.h"
#include "bigflow_python/python_args.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/objectors/serde_impl_wrapper.h"

namespace baidu {
namespace bigflow {
namespace python {

KVSerdeFn::KVSerdeFn(): _buffer(NULL)
                        , _buffer_size(0) {}

KVSerdeFn::~KVSerdeFn() {
    if (_buffer != NULL) {
        delete []_buffer;
        _buffer = NULL;
    }
    _objectors.clear();
}

void KVSerdeFn::Setup(const std::string& config) {
    boost::python::object py_obj = CPickleSerde().loads(config);
    CHECK(is_tuple(py_obj)) << "KV Serde config parameter must be tuple";
    CHECK_GE(tuple_len(py_obj), 2) << "KV Serde must be large than 2";
    int64_t len = tuple_len(py_obj);
    for (int serde_ind = 0; serde_ind < len; ++serde_ind) {
        _objectors.push_back(
                create_serde_impl_wrapper(tuple_get_item(py_obj, serde_ind)));
    }

    _buffer = new char[INIT_BUFFER_SIZE];
    _buffer_size = INIT_BUFFER_SIZE;
}

void KVSerdeFn::apply_kv_object(const boost::python::object& apply_obj,
        flume::core::Emitter* emitter) {
    CHECK(is_tuple(apply_obj) || is_list(apply_obj));
    int serde_size = _objectors.size();
    int data_len = tuple_len(apply_obj);
    int valid_len = std::min(serde_size, data_len);
    _tuple = new_tuple(valid_len);
    for (int data_ind = 0; data_ind < valid_len; ++data_ind) {
        apply_single_serde(_objectors[data_ind],
                tuple_get_item(apply_obj, data_ind), data_ind);
    }
    emitter->Emit(&_tuple);
}

void KVSerdeFn::call(void* object, flume::core::Emitter* emitter) {
    PythonArgs* args = static_cast<PythonArgs*>(object);
    CHECK_NOTNULL(args);
    CHECK_NOTNULL(args->args);
    CHECK(_buffer != NULL) << "Buffer must be initialize before serialize";

    boost::python::object apply_obj = tuple_get_item(*(args->args), 0);
    apply_kv_object(apply_obj, emitter);
}

void KVSerializeFn::apply_single_serde(
        SerdeImplWrapper& serde,
        const boost::python::object& apply_obj,
        int data_ind) {
    char* serde_buf = _buffer;
    uint32_t need_size = serde.serialize((void*)&apply_obj, serde_buf, _buffer_size);
    toft::scoped_ptr<char>  tmp_buffer;
    if (need_size > _buffer_size) {
        tmp_buffer.reset(new(std::nothrow) char[need_size]);
        CHECK(NULL != tmp_buffer.get()) << "Not enough serde buffer allocated";
        need_size = serde.serialize((void*)&apply_obj, tmp_buffer.get(), need_size);
        serde_buf = tmp_buffer.get();
    }
    boost::python::str result(serde_buf, need_size);
    tuple_set_item(_tuple, data_ind, result);
}

void KVDeserializeFn::apply_single_serde(
        SerdeImplWrapper& serde,
        const boost::python::object& apply_obj,
        int data_ind) {
    char* buf = NULL;
    Py_ssize_t size = 0;
    PyString_AsStringAndSize(apply_obj.ptr(), &buf, &size);
    void *py_obj = serde.deserialize(buf, size);
    boost::python::object result =
        *static_cast<boost::python::object*>(py_obj);
    tuple_set_item(_tuple, data_ind, result);
    serde.release_object(py_obj);
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu


