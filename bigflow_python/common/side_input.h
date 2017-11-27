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
// Author: YE, Xianjin<yexianjin@baidu.com>
//
// SideInput definitions.
//

#ifndef BIGFLOW_PYTHON_COMMON_SIDE_INPUT_H
#define BIGFLOW_PYTHON_COMMON_SIDE_INPUT_H

#include "boost/python.hpp"

#include <vector>

#include "flume/core/iterator.h"

namespace baidu {
namespace bigflow {
namespace python {

class SideInput {
public:
    SideInput(flume::core::Iterator* iterator) : _iterator(iterator), _already_get_list_iterator(false),
                                                 _already_get_vec_iterator(false){
    }

    SideInput(const boost::python::list& list)
        : _iterator(NULL), _already_get_list_iterator(true), _already_get_vec_iterator(true), _side_input_list(list) {
    }

    boost::python::object length();

    boost::python::object get_iterator();

    boost::python::object get_item(int n);

    boost::python::object get_vec_item(int n);

    boost::python::object as_list();

    const std::vector<boost::python::object>& as_vector();

private:
    flume::core::Iterator* _iterator;
    bool _already_get_list_iterator;
    bool _already_get_vec_iterator;
    boost::python::list _side_input_list;
    std::vector<boost::python::object> _side_input_vec;
};

} // namespace python
} // namespace bigflow
} // namespace baidu
#endif //BIGFLOW_PYTHON_COMMON_SIDE_INPUT_H
