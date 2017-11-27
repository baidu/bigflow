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
// Author: Zhang Yuncong(zhangyuncong@baidu.com)
//

#ifndef PY_FUNCTOR_CALLER_H
#define PY_FUNCTOR_CALLER_H

#include "boost/python.hpp"

#include <vector>

#include "glog/logging.h"
#include "toft/base/closure.h"
#include "toft/base/scoped_ptr.h"

#include "bigflow_python/functors/functor.h"

namespace baidu {
namespace bigflow {
namespace python {

// This is a tool class. It provided some convenient methods to call a Functor.
class PyFunctorCaller {
public:
    PyFunctorCaller(Functor* fn);

    // call the functor by args
    boost::python::object operator()(toft::Closure<void ()>* done_fn,
                                     boost::python::tuple* args = NULL,
                                     boost::python::dict* key_args = NULL);

    boost::python::object call(bool* done,
                               boost::python::tuple* args = NULL,
                               boost::python::dict* key_args = NULL);

    void operator() (flume::core::Emitter* emitter,
                     boost::python::tuple* args = NULL,
                     boost::python::dict* key_args = NULL);

    ~PyFunctorCaller();

private:
    class Impl;
    toft::scoped_ptr<Impl> _impl;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // PY_FUNCTOR_CALLER_H
