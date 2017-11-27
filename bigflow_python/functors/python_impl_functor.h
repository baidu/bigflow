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
/**
* @created:     2015/06/18
* @filename:    python_impl_functor.h
* @author:      bigflow-opensource@baidu.com
* @brief:       python impl functor implementation
*/

#ifndef BIGFLOW_PYTHON_PYTHON_IMPL_FUNCTOR_H_
#define BIGFLOW_PYTHON_PYTHON_IMPL_FUNCTOR_H_

#include <vector>

#include "boost/python.hpp"
#include "glog/logging.h"
#include "toft/base/scoped_ptr.h"

#include "bigflow_python/functors/functor.h"
#include "bigflow_python/python_args.h"
#include "bigflow_python/proto/processor.pb.h"
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

// This class is used for converting a python function to a Functor
// If user uses a python function in transforms, such as p.map(lambda x: x + 1),
// the lambda function will be wrapped by the PythonImplFunctor,
// then be used in the C++ code to process the data.
class PythonImplFunctor : public Functor {
public:
    PythonImplFunctor() {}
    virtual ~PythonImplFunctor() {}

public:
    // setup the functor
    // the config should be a cloudpickle serialized python function.
    virtual void Setup(const std::string& config);

    virtual void call(void* object, flume::core::Emitter* emitter);

private:
    boost::python::object _fn;
    bool _expect_iterable;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_PYTHON_PYTHON_IMPL_FUNCTOR_H_
