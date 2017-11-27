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
// Author: Zhang Yuncong <bigflow-opensource@baidu.com>
//
// Accumulate Processor

#include "bigflow_python/processors/accumulate_processor.h"

#include <exception>

#include "glog/logging.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/python_args.h"
#include "bigflow_python/functors/functor.h"

namespace baidu {
namespace bigflow {
namespace python {

void AccumulateProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
    _zero_fn.reset(new PyFunctorCaller(fns[0]));
    _fn.reset(new PyFunctorCaller(fns[1]));
    _already_new_tuple = false;
}

void AccumulateProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                      ProcessorContext* context) {
    _context = context;
    _done_fn.reset(toft::NewPermanentClosure(context, &ProcessorContext::done));
    const std::vector<boost::python::object>& side_inputs = context->python_side_inputs();
    boost::python::tuple tuple = construct_args(0, side_inputs);
    int64_t size = side_inputs.size();
    if (!_already_new_tuple) {
        _normal_args = new_tuple(size + 2);
        _already_new_tuple = true;
    }
    tuple_set_item(_normal_args, 0, (*_zero_fn)(_done_fn.get(), &tuple));
    fill_args(2, side_inputs, &_normal_args);
}

void AccumulateProcessor::process(void* object) {
    tuple_set_item(_normal_args, 1, *static_cast<boost::python::object*>(object));
    tuple_set_item(_normal_args, 0, (*_fn)(_done_fn.get(), &_normal_args));
}

void AccumulateProcessor::end_group() {
    boost::python::object ret = tuple_get_item(_normal_args, 0);
    _context->emit(&ret);
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

