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
/**
* @created:     2015/06/18
* @filename:    python_impl_functor.cpp
* @author:      bigflow-opensource@baidu.com
* @brief:       python impl functor implementation
*/

#include "bigflow_python/common/python.h"

#include "bigflow_python/functors/python_impl_functor.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/python_interpreter.h"

namespace baidu {
namespace bigflow {
namespace python {

void PythonImplFunctor::Setup(const std::string& config) {
    boost::python::object conf = CPickleSerde().loads(config);
    _fn = conf["fn"];
    _expect_iterable = conf["expect_iterable"];
}

void PythonImplFunctor::call(void*object, flume::core::Emitter* emitter) {
    CHECK_NOTNULL(emitter);
    PythonArgs* args = static_cast<PythonArgs*>(object);

    // Do Not Support kargs for now
    boost::python::object ret = _fn(*(*args->args));

    if (_expect_iterable) {
        if (is_list(ret)) {
            int length = boost::python::len(ret);
            for (int i = 0; i < length; i++) {
                boost::python::object item = list_get_item(ret, i);
                if (!emitter->Emit(&item)) {
                    emitter->Done();
                }
            }
        } else {
            PyObject* iter = PyObject_GetIter(ret.ptr());
            if (iter == NULL) {
                if (PyErr_Occurred()) {
                    BIGFLOW_HANDLE_PYTHON_EXCEPTION();
                } else {
                    BIGFLOW_PYTHON_RAISE_EXCEPTION(
                        "the fn used in flat_map must return an iterable object. \n"
                        "Have you returned something which is not iterable (Such as None)?\n"
                        "Search the document with keywords "
                        "\"transforms.flat_map\" for more information."
                    );
                }
            }

            PyObject* item = NULL;
            while ((item = PyIter_Next(iter)) != NULL) {
                boost::python::object obj =
                    boost::python::object(boost::python::handle<>(item));
                if (!emitter->Emit(&obj)) {
                    emitter->Done();
                }
            }
            Py_DECREF(iter);
            if (PyErr_Occurred()) {
                BIGFLOW_HANDLE_PYTHON_EXCEPTION();
            }
        }
    } else {
        if (!emitter->Emit(&ret)) {
            emitter->Done();
        }
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu
