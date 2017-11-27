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

#include "bigflow_python/common/python.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/serde/cpickle_serde.h"

#include "glog/logging.h"
#include "toft/base/scoped_ptr.h"

namespace baidu {
namespace bigflow {
namespace python {

class CPickleSerde::Impl {
public:
    Impl(bool python_interpreter_already_inited) {
        if (!python_interpreter_already_inited) {
            PythonInterpreter::Instance();
        }
        try {
            _pickle = ::boost::python::import("cPickle");
            _loads = _pickle.attr("loads");
            _dumps = _pickle.attr("dumps");
        } catch(boost::python::error_already_set) {
            LOG(INFO) << "Error in CPickleSerde implementation";
            BIGFLOW_HANDLE_PYTHON_EXCEPTION();
        }
    }

    boost::python::object loads(const std::string& py_serialized) const {
        return _loads(py_serialized);
    }

    boost::python::object loads(const char * py_str, uint32_t size) const {
        return loads(std::string(py_str, size));
    }

    boost::python::object dumps(boost::python::object& py_obj) const {
        return _dumps(py_obj);
    }

private:
    boost::python::object _loads;
    boost::python::object _dumps;
    boost::python::object _pickle;
};

CPickleSerde::CPickleSerde(bool python_interpreter_already_inited)
        : _impl(new CPickleSerde::Impl(python_interpreter_already_inited)) {
}

CPickleSerde::CPickleSerde() : _impl(new CPickleSerde::Impl(false)) {
}

boost::python::object CPickleSerde::loads(const std::string& py_serialized) const {
    return _impl->loads(py_serialized);
}

boost::python::object CPickleSerde::loads(const char * py_str, uint32_t size) const {
    return _impl->loads(py_str, size);
}

boost::python::object CPickleSerde::dumps(boost::python::object& py_obj) const {
    return _impl->dumps(py_obj);
}

CPickleSerde::~CPickleSerde() {}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

