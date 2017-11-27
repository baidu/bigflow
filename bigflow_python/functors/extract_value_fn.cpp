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
* @author:      zhangyuncong (zhangyuncong@baidu.com)
* @brief:       ExtractValue
*/

#include "boost/python.hpp"

#include "bigflow_python/functors/extract_value_fn.h"

#include "bigflow_python/python_args.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/python_interpreter.h"

namespace baidu {
namespace bigflow {
namespace python {

ExtractValueFn::ExtractValueFn() {}

void ExtractValueFn::Setup(const std::string& config){
}

void ExtractValueFn::call(void* object, flume::core::Emitter* emitter) {
    PythonArgs* args = static_cast<PythonArgs*>(object);
    boost::python::object obj = tuple_get_item(*(args->args), 0);
    int len = object_len(obj);
    if (len != 2) {
        std::string msg = "Your record: ["
                + str_get_buf(boost::python::str(obj)).as_string()
                + "](type=" + type_string(obj)
                + ") passed to cogroup/join/group_by_key is invalid. Reason: \n"
                "Each record in the pcollection passed to the cogroup/join/group_by_key operations "
                "should have exactly two elements. \nYou may convert it into a tuple or a list,"
                " which contains two elements inside. \n Eg.\n "
                "_pipeline.parallelize([(1, 2), (3, 4), (5, 6)]).group_by_key().get() #OK\n"
                "_pipeline.parallelize([(1, 2, 3), (4, 5, 6), (7, 8, 9)]).group_by_key().get() #Error\n"
                "_pipeline.parallelize([1, 2]).group_by_key().get()  #Error\n";
        BIGFLOW_HANDLE_PYTHON_EXCEPTION_WITH_ERROR_MESSAGE(msg);
    }
    boost::python::object tmp = object_get_item(obj, 1);
    emitter->Emit(&tmp);
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu


