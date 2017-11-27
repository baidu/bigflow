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
// Author: Zhang Jianwei <bigflow-opensource@baidu.com>
//
// python environment

#include "boost/python.hpp"
#include "boost/ptr_container/ptr_vector.hpp"

#include "bigflow_python/processors/python_environment.h"
#include "flume/runtime/counter.h"

namespace baidu {
namespace bigflow {
namespace python {

using flume::runtime::Counter;
using flume::runtime::CounterSession;

void PythonEnvironment::Setup(const std::string& config) {
}

// execuate when task setup.
void PythonEnvironment::do_setup() {
}

// execuate when task cleanup.
void PythonEnvironment::do_cleanup() {
    try {
        boost::python::object counter = boost::python::import("bigflow.counter");
        boost::python::dict counters_py = boost::python::extract<boost::python::dict>(counter.attr("counter_dict"));

        boost::python::list keys = counters_py.keys();
        for (int i = 0; i < len(keys); ++i) {
            boost::python::extract<std::string> extracted_key(keys[i]);
            CHECK(extracted_key.check());
            std::string key = extracted_key;
            boost::python::extract<uint64_t> extracted_val(counters_py[key]);
            CHECK(extracted_val.check());
            uint64_t value = extracted_val;
            std::string prefix, name;
            CounterSession::GetPrefixFromCounterKey(key, &prefix, &name);
            CounterSession::GlobalCounterSession()->GetCounter(prefix, name)->Update(value);
            counters_py[key] = boost::python::object(0);
        }
    } catch (boost::python::error_already_set&) {
        CHECK(false) << "UpdateCounter error_already_set exception";
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

