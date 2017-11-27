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
// Author: Zhang Yuncong (bigflow-opensource@baidu.com)
// A tool to impl some simple functor

#ifndef BLADE_BIGFLOW_PYTHON_FUNCTORS_FUNCTION_H
#define BLADE_BIGFLOW_PYTHON_FUNCTORS_FUNCTION_H

#include <string>

#include "bigflow_python/functors/functor.h"
#include "bigflow_python/common/python.h"

namespace baidu {
namespace bigflow {
namespace python {

template<int N>
class Function;

template<>
class Function<0> : public Functor {
public:
    virtual void Setup(const std::string& config){}
    virtual boost::python::object operator()() = 0;
    virtual ~Function() {}

    virtual void call(void* object, flume::core::Emitter* emitter) {
        boost::python::object ret = this->operator()();
        if (emitter->Emit(&ret)) {
            emitter->Done();
        }
    }
};

template<>
class Function<1> : public Functor {
public:
    virtual void Setup(const std::string& config){}
    virtual boost::python::object operator()(boost::python::object arg0) = 0;
    virtual ~Function() {}

    virtual void call(void* object, flume::core::Emitter* emitter) {
        CHECK_NOTNULL(object);
        boost::python::tuple* args = static_cast<PythonArgs*>(object)->args;
        CHECK_NOTNULL(args);
        boost::python::object arg0 = tuple_get_item(*args, 0);

        boost::python::object ret = this->operator()(arg0);
        if (emitter->Emit(&ret)) {
            emitter->Done();
        }
    }
};

template<>
class Function<2> : public Functor {
public:
    virtual void Setup(const std::string& config){}
    virtual boost::python::object operator()(boost::python::object arg0,
                                             boost::python::object arg1) = 0;
    virtual ~Function() {}

    virtual void call(void* object, flume::core::Emitter* emitter) {
        CHECK_NOTNULL(object);
        boost::python::tuple* args = static_cast<PythonArgs*>(object)->args;
        CHECK_NOTNULL(args);
        boost::python::object arg0 = tuple_get_item(*args, 0);
        boost::python::object arg1 = tuple_get_item(*args, 1);

        boost::python::object ret = this->operator()(arg0, arg1);
        if (emitter->Emit(&ret)) {
            emitter->Done();
        }
    }
};

template<>
class Function<3> : public Functor {
public:
    virtual void Setup(const std::string& config){}
    virtual boost::python::object operator()(boost::python::object arg0,
                                             boost::python::object arg1,
                                             boost::python::object arg2) = 0;
    virtual ~Function() {}

    virtual void call(void* object, flume::core::Emitter* emitter) {
        CHECK_NOTNULL(object);
        boost::python::tuple* args = static_cast<PythonArgs*>(object)->args;
        CHECK_NOTNULL(args);
        boost::python::object arg0 = tuple_get_item(*args, 0);
        boost::python::object arg1 = tuple_get_item(*args, 1);
        boost::python::object arg2 = tuple_get_item(*args, 2);

        boost::python::object ret = this->operator()(arg0, arg1, arg2);
        if (emitter->Emit(&ret)) {
            emitter->Done();
        }
    }
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BLADE_BIGFLOW_PYTHON_FUNCTORS_FUNCTION_H
