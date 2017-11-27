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
// Author: Pan Yunhong <bigflow-opensource@baidu.com>
//
// Base classes for User Defined Objector and implementation

#ifndef BIGFLOW_PYTHON_PYTHON_OBJECTOR_H_
#define BIGFLOW_PYTHON_PYTHON_OBJECTOR_H_

#include "boost/python.hpp"

#include <exception>

#include "boost/ptr_container/ptr_vector.hpp"
#include "glog/logging.h"

#include "flume/core/objector.h"
#include "flume/core/entity.h"

#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/common/python.h"

namespace baidu {
namespace bigflow {
namespace python {

class PythonObjector {
public:
    virtual void setup(const std::string& config) = 0;

    virtual uint32_t serialize(void* object, char* buffer, uint32_t buffer_size) = 0;

    virtual void* deserialize(const char* buffer, uint32_t buffer_size) = 0;

    virtual void release(void* object) = 0;

    virtual ~PythonObjector() {}
};

template<typename ObjectorType>
class PythonObjectorImpl : public flume::core::Objector {
public:
    PythonObjectorImpl() {}

    virtual void Setup(const std::string& config);

    virtual uint32_t Serialize(void* object, char* buffer, uint32_t buffer_size);

    virtual void* Deserialize(const char* buffer, uint32_t buffer_size);

    virtual void Release(void* object);
private:
    ObjectorType _objector;
};

template<typename ObjectorType>
void PythonObjectorImpl<ObjectorType>::Setup(const std::string& config) {
    try {
        _objector.setup(config);
    } catch (boost::python::error_already_set&) {
        LOG(INFO) << __PRETTY_FUNCTION__ << " failed";
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

template<typename ObjectorType>
uint32_t PythonObjectorImpl<ObjectorType>::Serialize(void* object, char* buffer, \
    uint32_t buffer_size) {
    uint32_t result = 0;
    try {
        result = _objector.serialize(object, buffer, buffer_size);
    } catch (boost::python::error_already_set&) {
        LOG(INFO) << __PRETTY_FUNCTION__ << " failed";
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
    return result;
}

template<typename ObjectorType>
void* PythonObjectorImpl<ObjectorType>::Deserialize(const char* buffer, uint32_t buffer_size) {
    void *result = NULL;
    try {
        result = _objector.deserialize(buffer, buffer_size);
    } catch (boost::python::error_already_set&) {
        LOG(INFO) << __PRETTY_FUNCTION__ << " failed";
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
    return result;
}

template<typename ObjectorType>
void PythonObjectorImpl<ObjectorType>::Release(void* object) {
    try {
        _objector.release(object);
    } catch (boost::python::error_already_set&) {
        LOG(INFO) << __PRETTY_FUNCTION__ << " failed";
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_PYTHON_PYTHON_OBJECTOR_H_
