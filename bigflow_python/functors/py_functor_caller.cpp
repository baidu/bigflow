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

#include "bigflow_python/functors/py_functor_caller.h"

#include <vector>

#include "flume/core/emitter.h"
#include "bigflow_python/python_args.h"

namespace baidu {
namespace bigflow {
namespace python {

namespace {

class HoldResultEmitter : public flume::core::Emitter{
public:
    typedef toft::Closure<void ()>* DoneFn;

    bool Emit(void* object){
        boost::python::object* pyobj = static_cast<boost::python::object*>(object);
        _result = *pyobj;
        return true;
    }

    void Done() {
        _done_fn->Run();
    }

    boost::python::object clear_result() {
        boost::python::object ret = _result;
        _result = _none;
        return ret;
    }

    boost::python::object result() {
        return _result;
    }

    void set_done_fn(DoneFn fn) {
        _done_fn = fn;
    }

private:
    boost::python::object _result;
    boost::python::object _none;
    DoneFn _done_fn;
};

void set_true(bool* done) {
    *done = true;
}

} // namespace

class PyFunctorCaller::Impl {
public:
    Impl(Functor* fn) : _fn(fn), _done(false) {
        _set_done_fn.reset(toft::NewPermanentClosure(set_true, &_done));
        _emitter.set_done_fn(_set_done_fn.get());
    }

    boost::python::object operator()(toft::Closure<void ()>* done_fn,
                                     boost::python::tuple* args = NULL,
                                     boost::python::dict* key_args = NULL) {
        _emitter.set_done_fn(done_fn);
        (*this)(&_emitter, args, key_args);
        return _emitter.clear_result();
    }

    boost::python::object call(bool* done,
                               boost::python::tuple* args = NULL,
                               boost::python::dict* key_args = NULL) {
        _done = false;
        (*this)(&_emitter, args, key_args);
        if (done != NULL) {
            *done = _done;
        }
        return _emitter.clear_result();
    }

    void operator() (flume::core::Emitter* emitter,
                     boost::python::tuple* args = NULL,
                     boost::python::dict* key_args = NULL) {
        _args.args = args;
        _args.kargs = key_args;
        _fn->call(&_args, emitter);
    }

private:
    Functor* _fn;
    HoldResultEmitter _emitter;
    PythonArgs _args;

    toft::scoped_ptr<toft::Closure<void ()> > _set_done_fn;
    bool _done;
};

PyFunctorCaller::PyFunctorCaller(Functor* fn) : _impl(new Impl(fn)) {}

boost::python::object PyFunctorCaller::operator()(toft::Closure<void ()>* done_fn,
                                 boost::python::tuple* args,
                                 boost::python::dict* key_args) {
    return (*_impl)(done_fn, args, key_args);
}

void PyFunctorCaller::operator() (flume::core::Emitter* emitter,
                                  boost::python::tuple* args,
                                  boost::python::dict* key_args) {
    (*_impl)(emitter, args, key_args);
}

boost::python::object PyFunctorCaller::call(bool* done,
                                     boost::python::tuple* args,
                                     boost::python::dict* key_args) {

    return _impl->call(done, args, key_args);
}

PyFunctorCaller::~PyFunctorCaller() {}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

