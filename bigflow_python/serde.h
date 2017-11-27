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
// Author: Zhang Yuncong (bigflow-opensource@baidu.com)
//

#ifndef BIGFLOW_PYTHON_SERDE_H_
#define BIGFLOW_PYTHON_SERDE_H_

#include "boost/python.hpp"

namespace baidu {
namespace bigflow {
namespace python {

// Serde Base, to serialize and deserialize python object
// TODO: The name is confused, rename this class to another name.
class Serde {
public:
    // load object from string
    virtual boost::python::object loads(const std::string& py_serialized) const = 0;

    // load object from string
    virtual boost::python::object loads(const char * py_str, uint32_t size) const = 0;

    // dump object to python str.
    virtual boost::python::object dumps(boost::python::object& py_obj) const = 0;

    virtual ~Serde() {}
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif //  BIGFLOW_PYTHON_SERDE_H_
