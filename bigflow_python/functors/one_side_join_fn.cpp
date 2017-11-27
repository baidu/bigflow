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
* @brief:       OneSideJoin
*/

#include "boost/python.hpp"

#include "bigflow_python/functors/one_side_join_fn.h"

#include "bigflow_python/python_args.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/python_interpreter.h"


namespace baidu {
namespace bigflow {
namespace python {

OneSideJoinFn::OneSideJoinFn() {}

void OneSideJoinFn::Setup(const std::string& config){
}

class MakeTupleAndEmit{
public:
    MakeTupleAndEmit(const boost::python::object& left, flume::core::Emitter* emitter)
        : _emitter(emitter),
        _left(left) {
    }

    void operator()(const boost::python::object& object) {
        boost::python::object tmp = boost::python::make_tuple(_left, object);
        _emitter->Emit(&tmp);
    }

    ~MakeTupleAndEmit() {}

private:
    flume::core::Emitter* _emitter;
    const boost::python::object& _left;
};

void OneSideJoinFn::call(void* object, flume::core::Emitter* emitter) {
    PythonArgs* args = static_cast<PythonArgs*>(object);
    boost::python::tuple nargs = *(args->args);
    boost::python::object left = tuple_get_item(nargs, 0);
    boost::python::object right = tuple_get_item(nargs, 1);

    int len = vec_foreach(side_input_vec(right), MakeTupleAndEmit(left, emitter));
    if (len == 0) {
        boost::python::tuple tmp = boost::python::make_tuple(left, boost::python::object());
        emitter->Emit(&tmp);
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu


