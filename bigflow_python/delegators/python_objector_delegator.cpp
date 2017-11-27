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
// Author: Wang Cong <wangcong09@baidu.com>
//
// A delegating class for python Objectors.

#include "python_objector_delegator.h"

#include "string.h"

#include "marshal.h"
#include "glog/logging.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

void PythonObjectorDelegator::Setup(const std::string& config) {
    try {
        _objector = CPickleSerde().loads(config);
        _serialize = _objector.attr("serialize");
        _deserialize = _objector.attr("deserialize");
    } catch (boost::python::error_already_set) {
        LOG(INFO) << "objector has no attr \"serialize\" or \"deserialize\"??";
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

uint32_t PythonObjectorDelegator::Serialize(void* object, char* buffer, uint32_t buffer_size) {
    try {
        boost::python::object* obj = static_cast<boost::python::object*>(object);
        boost::python::object result = _serialize(*obj);
        PyObject* serialized = result.ptr();

        char* buf = NULL;
        Py_ssize_t size = 0;
        PyString_AsStringAndSize(serialized, &buf, &size);
        if (size <= buffer_size) {
            memcpy(buffer, buf, size);
        }
        return size;
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

void* PythonObjectorDelegator::Deserialize(const char* buffer, uint32_t buffer_size) {
    try {
        boost::python::object deserialized = _deserialize(boost::python::str(buffer, buffer_size));
        return new boost::python::object(deserialized);
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

void PythonObjectorDelegator::Release(void* object) {
    delete static_cast<boost::python::object*>(object);
}

void MarshalObjector::Setup(const std::string& config) {
}

uint32_t MarshalObjector::Serialize(void* object, char* buffer, uint32_t buffer_size) {
    try {
        // TODO:
        return 0;
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

void* MarshalObjector::Deserialize(const char* buffer, uint32_t buffer_size) {
    try {
        // TODO:
        return NULL;
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

void MarshalObjector::Release(void* object) {
    // TODO:
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

