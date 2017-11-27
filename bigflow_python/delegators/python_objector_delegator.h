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
// Author: Wang Cong <bigflow-opensource@baidu.com>
//
// A delegating class for python Objectors.

#ifndef BIGFLOW_PYTHON_PYTHON_OBJECTOR_DELEGATOR_H_
#define BIGFLOW_PYTHON_PYTHON_OBJECTOR_DELEGATOR_H_

#include "boost/python.hpp"

#include "bigflow_python/python_interpreter.h"

#include "flume/core/objector.h"

namespace baidu {
namespace bigflow {
namespace python {

class PythonObjectorDelegator : public flume::core::Objector {
public:
    PythonObjectorDelegator() {}
    ~PythonObjectorDelegator() {}

    virtual void Setup(const std::string& config);

    // save key to buffer if has enough buffer
    // return key size
    virtual uint32_t Serialize(void* object, char* buffer, uint32_t buffer_size);

    // return an address to deserialized object.
    // buffer will have longger lifetime than returned object.
    virtual void* Deserialize(const char* buffer, uint32_t buffer_size);

    // release resources hold by object
    virtual void Release(void* object);

protected:
    boost::python::object _objector;

    boost::python::object _serialize;
    boost::python::object _deserialize;
};

class MarshalObjector : public flume::core::Objector {
public:
    MarshalObjector() {}
    ~MarshalObjector() {}

    virtual void Setup(const std::string& config);

    // save key to buffer if has enough buffer
    // return key size
    virtual uint32_t Serialize(void* object, char* buffer, uint32_t buffer_size);

    // return an address to deserialized object.
    // buffer will have longger lifetime than returned object.
    virtual void* Deserialize(const char* buffer, uint32_t buffer_size);

    // release resources hold by object
    virtual void Release(void* object);
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_PYTHON_PYTHON_OBJECTOR_DELEGATOR_H_
