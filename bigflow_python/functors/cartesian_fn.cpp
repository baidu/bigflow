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
* @filename:    python_impl_functor.h
* @author:      zhangyuncong (bigflow-opensource@baidu.com)
*/

#include "bigflow_python/functors/cartesian_fn.h"

#include "boost/python.hpp"

#include "bigflow_python/common/python.h"
#include "bigflow_python/delegators/python_processor_delegator.h"
#include "bigflow_python/python_args.h"
#include "bigflow_python/python_interpreter.h"

namespace baidu {
namespace bigflow {
namespace python {

CartesianFn::CartesianFn() {}

void CartesianFn::Setup(const std::string& config) {
}

void CartesianFn::call(void* object, flume::core::Emitter* emitter) {
    PythonArgs* args = static_cast<PythonArgs*>(object);
    CHECK_NOTNULL(args);
    CHECK_NOTNULL(args->args);
    PyObject* obj = args->args->ptr();
    PyObject* left_obj = PyTuple_GetItem(obj, 0);
    PyObject* right_obj = PyTuple_GetItem(obj, 1);

    boost::python::object left((boost::python::handle<>(boost::python::borrowed(left_obj))));
    boost::python::object right((boost::python::handle<>(boost::python::borrowed(right_obj))));
    SideInput& side_input = boost::python::extract<SideInput&>(right);
    const std::vector<boost::python::object>& vec = side_input.as_vector();

    for (unsigned int i = 0; i < vec.size(); i++) {
        boost::python::tuple temp = boost::python::make_tuple(left, vec[i]);
        if (!emitter->Emit(&temp)) {
            emitter->Done();
            break;
        }
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu


