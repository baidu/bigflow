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
// Author: bigflow-opensource@baidu.com
//
// A delegating class for python time readers.

#include "python_time_reader_delegator.h"

#include "string.h"

#include "glog/logging.h"

#include "flume/core/entity.h"
#include "flume/proto/entity.pb.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/functors/functor.h"
#include "bigflow_python/functors/py_functor_caller.h"
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

class PythonTimeReaderDelegator::Impl {
public:
    void Setup(const std::string& config) {
        flume::PbEntity fn_entity;
        CHECK(fn_entity.ParseFromArray(config.data(), config.size()));
        _fn.reset(flume::core::Entity<Functor>::From(fn_entity).CreateAndSetup());
        _fn_caller.reset(new PyFunctorCaller(_fn.get()));
        _args = new_tuple(1);
    }

    uint64_t get_timestamp(void* object) {
        tuple_set_item(_args, 0, *static_cast<boost::python::object*>(object));
        boost::python::object ret = _fn_caller->call(NULL, &_args);
        return boost::python::extract<uint64_t>(ret);
    }

private:
    toft::scoped_ptr<Functor> _fn;
    toft::scoped_ptr<PyFunctorCaller> _fn_caller;
    boost::python::tuple _args;
};

void PythonTimeReaderDelegator::Setup(const std::string& config) {
    try {
        _impl.reset(new Impl);
        _impl->Setup(config);
    } catch (boost::python::error_already_set&) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

uint64_t PythonTimeReaderDelegator::get_timestamp(void* object) {
    try {
        return _impl->get_timestamp(object);
    } catch (boost::python::error_already_set&) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
    return 0;
}

PythonTimeReaderDelegator::~PythonTimeReaderDelegator() {}
PythonTimeReaderDelegator::PythonTimeReaderDelegator() {}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

