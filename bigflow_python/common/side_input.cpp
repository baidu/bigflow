/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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
// Author: YE, Xianjin<bigflow-opensource@baidu.com>
//
// SideInput Implementation.
//

#include "bigflow_python/common/side_input.h"
#include "bigflow_python/common/python.h"
#include "glog/logging.h"

namespace baidu {
namespace bigflow {
namespace python {

boost::python::api::object SideInput::get_iterator() {
    try {
        return as_list().attr("__iter__")();
    } catch ( boost::python::error_already_set ) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

boost::python::object SideInput::length() {
    std::vector<boost::python::object> side_input_vec = as_vector();
    return boost::python::api::object(side_input_vec.size());
}

boost::python::object SideInput::get_item(int n) {
    std::vector<boost::python::object> side_input_vec = as_vector();
    CHECK_LT(n, side_input_vec.size());
    return side_input_vec[n];
}

boost::python::object SideInput::as_list() {
    try {
        if (_already_get_list_iterator) {
            return _side_input_list;
        }
        if (!_already_get_vec_iterator) {
            as_vector();
        }
        _already_get_list_iterator = true;
        for (unsigned int i = 0; i < _side_input_vec.size(); i++) {
            _side_input_list.append(_side_input_vec[i]);
        }
        return _side_input_list;
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

const std::vector<boost::python::object>& SideInput::as_vector() {
    try {
        if (_already_get_vec_iterator) {
            return _side_input_vec;
        }
        _already_get_vec_iterator = true;
        _iterator->Reset();
        while (_iterator->HasNext()) {
            boost::python::object* pyobj = static_cast<boost::python::api::object*>(_iterator->NextValue());
            _side_input_vec.push_back(*pyobj);
        }
        _iterator->Done();
        return _side_input_vec;
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

} // namespace python
} // namespace bigflow
} // namespace baidu
