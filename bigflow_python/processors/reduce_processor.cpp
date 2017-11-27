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
* @created:     2015/06/22
* @filename:    reduce_processor.cpp
* @author:      bigflow-opensource@baidu.com
* @brief:       reduce processor implementation
*/

#include "bigflow_python/processors/reduce_processor.h"

#include "glog/logging.h"

#include "bigflow_python/proto/processor.pb.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/functors/functor.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

static const int NORMAL_ARGS_NUMBER = 2;

void ReduceProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
    _fn.reset(new PyFunctorCaller(fns[0]));
    _already_new_tuple = false;
}

void ReduceProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                   ProcessorContext* context) {
    _context = context;
    _done_fn.reset(toft::NewPermanentClosure(context, &ProcessorContext::done));
    _is_first_record = true;
    _already_called_reduce_fn = false;
    if (!_already_new_tuple) {
        _args = new_tuple(NORMAL_ARGS_NUMBER + context->python_side_inputs().size());
        _already_new_tuple = true;
    }
    fill_args(NORMAL_ARGS_NUMBER, context->python_side_inputs(), &_args);
}

void ReduceProcessor::process(void* object) {
    boost::python::object normal_args = *static_cast<boost::python::object*>(object);
    if (!_is_first_record) {
        if (!_already_called_reduce_fn) {
            tuple_set_item(_args, 0, deep_copy(tuple_get_item(_args, 0)));
            _already_called_reduce_fn = true;
        }
        tuple_set_item(_args, 1, normal_args);
        tuple_set_item(_args, 0, (*_fn)(_done_fn.get(), &_args));
    } else {
        tuple_set_item(_args, 0, normal_args);
        _is_first_record = false;
    }
}

void ReduceProcessor::end_group() {
    if (!_is_first_record) {
        boost::python::object tmp = tuple_get_item(_args, 0);
        _context->emit(&tmp);
    }
}

} // namespace python
} // namespace bigflow
} // namespace baidu
