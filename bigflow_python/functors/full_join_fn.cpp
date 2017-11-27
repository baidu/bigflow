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
* @author:      zhangyuncong (zhangyuncong@baidu.com)
* @brief:       OneSideJoin
*/

#include "boost/python.hpp"

#include "bigflow_python/functors/full_join_fn.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/delegators/python_processor_delegator.h"
#include "bigflow_python/python_args.h"

namespace baidu {
namespace bigflow {
namespace python {

class EmitTuple {
public:
    EmitTuple(const boost::python::object& left, const boost::python::object& emit)
            : _left(left),
            _emit(emit) {
    }

    void operator() (const boost::python::object& right) {
        _emit(boost::python::make_tuple(_left, right));
    }

private:
    boost::python::object _left;
    boost::python::object _emit;
};

FullJoinInitializeFn::FullJoinInitializeFn() {}

void FullJoinInitializeFn::Setup(const std::string& config){
}

void FullJoinInitializeFn::call(void* object, flume::core::Emitter* emitter) {
    boost::python::object left_is_empty(true);
    emitter->Emit(&left_is_empty);
}

FullJoinTransformFn::FullJoinTransformFn() {}

void FullJoinTransformFn::Setup(const std::string& config){
}

void FullJoinTransformFn::call(void* object, flume::core::Emitter* ret_emitter) {
    PythonArgs* input_args = static_cast<PythonArgs*>(object);
    boost::python::tuple& args = *input_args->args;
    boost::python::object emit = tuple_get_item(args, 1).attr("emit");
    boost::python::object left = tuple_get_item(args, 2);
    boost::python::object right = tuple_get_item(args, 3);

    int len = vec_foreach(side_input_vec(right), EmitTuple(left, emit));

    if (len == 0) {
        boost::python::tuple tmp = boost::python::make_tuple(left, boost::python::object());
        emit(tmp);
    }

    boost::python::object left_is_empty(false);
    ret_emitter->Emit(&left_is_empty);
}

FullJoinFinalizeFn::FullJoinFinalizeFn() {}

void FullJoinFinalizeFn::Setup(const std::string& config){
}

void FullJoinFinalizeFn::call(void* object, flume::core::Emitter* ) {
    PythonArgs* input_args = static_cast<PythonArgs*>(object);
    boost::python::tuple& args = *input_args->args;
    boost::python::object left_is_empty = tuple_get_item(args, 0);
    boost::python::object emit = tuple_get_item(args, 1).attr("emit");
    boost::python::object right = tuple_get_item(args, 2);
    boost::python::object none;
    if (boost::python::extract<bool>(left_is_empty)) {
        vec_foreach(side_input_vec(right), EmitTuple(none, emit));
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu


