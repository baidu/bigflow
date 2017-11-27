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
* @created:     2016/11/03
* @filename:    split_string_to_types.cpp
* @author:      zhenggonglin(bigflow-opensource@baidu.com)
*/

#include "bigflow_python/functors/split_string_to_types.h"

#include <iostream>

namespace baidu {
namespace bigflow {
namespace python {

SplitStringToTypes::SplitStringToTypes():
    _ignore_overflow(false),
    _ignore_illegal_line(false) {}

void SplitStringToTypes::Setup(const std::string& config) {
    // config:
    //  separator: separator of line.
    //  fields_type: each columns types
    //  ignore_overflow: the flag of ignore overflow columns
    //  ignore_illegal_line: the flag of ignore illegal line.
    boost::python::object py_obj = CPickleSerde().loads(config);

    CHECK(is_tuple(py_obj)) << "Split string to types " \
        "config parameter must be tuple.";
    CHECK(tuple_len(py_obj) >= 4) << "The number of " \
        "Split string to types config parameter " \
        "must be more than four.";

    _sep = boost::python::extract<std::string>( \
            tuple_get_item(py_obj, 0));

    boost::python::object ftypes = tuple_get_item(py_obj, 1);

    CHECK(is_list(ftypes)) << "Fields types must be in a list.";

    int64_t fields_cnt = list_len(ftypes);

    for (int64_t i = 0; i < fields_cnt; i++) {
        boost::python::object t = list_get_item(ftypes, i);
        _fields_type.push_back(t);
    }

    _ignore_overflow = boost::python::extract<bool>(
            tuple_get_item(py_obj, 2));

    _ignore_illegal_line = boost::python::extract<bool>(
            tuple_get_item(py_obj, 3));

}

void SplitStringToTypes::call(
        void* object, flume::core::Emitter* emitter) {
    PythonArgs* args = static_cast<PythonArgs*>(object);
    CHECK_NOTNULL(args);
    CHECK_NOTNULL(args->args);

    boost::python::object apply_obj = tuple_get_item(*(args->args), 0);
    std::string line = boost::python::extract<std::string>(apply_obj);

    size_t start = 0;
    size_t index = 0;
    size_t sep_length = _sep.length();
    std::vector<std::string> parts;

    toft::SplitStringKeepEmpty(line, _sep, &parts);

    if (!_ignore_overflow) {
        CHECK_LE(parts.size(), _fields_type.size()) << \
                "Split string to types: field number more than fields_type number !";
    }

    if (!_ignore_illegal_line) {
        CHECK_GE(parts.size(), _fields_type.size()) << \
                "Split string to types: field number less than fields_type number !";
    } else {
        if (parts.size() < _fields_type.size()) {
            // ignore the illegal line.
            return;
        }
    }

    size_t slice_count = std::min(parts.size(), _fields_type.size());

    boost::python::tuple tp = new_tuple(slice_count);
    for (size_t i = 0; i < slice_count; i++) {
        boost::python::str pystr(parts[i]);
        tuple_set_item(tp, i, _fields_type[i](pystr));
    }

    if (!emitter->Emit(&tp)) {
        emitter->Done();
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

