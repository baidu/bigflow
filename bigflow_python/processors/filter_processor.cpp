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
* @created:     2015/06/19
* @filename:    filter_processor.cpp
* @author:      bigflow-opensource@baidu.com
* @brief:       filter processor implementation
*/

#include "bigflow_python/processors/filter_processor.h"

#include "glog/logging.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/proto/processor.pb.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/functors/functor.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

void FilterProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
    _fn.reset(new PyFunctorCaller(fns[0]));
    _already_new_tuple = false;
}

void FilterProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                   ProcessorContext* context) {
    _context = context;
    _done_fn.reset(toft::NewPermanentClosure(context, &ProcessorContext::done));
    //_args = construct_args(1, context->python_side_inputs());
    if (!_already_new_tuple) {
        _already_new_tuple = true;
        _args = new_tuple(1 + context->python_side_inputs().size());
    }
    fill_args(1, context->python_side_inputs(), &_args);
}

void FilterProcessor::process(void* object) {
    boost::python::object input = *static_cast<boost::python::object*>(object);
    tuple_set_item(_args, 0, input);
    boost::python::object ret = (*_fn)(_done_fn.get(), &_args);
    if (boost::python::extract<bool>(ret)) {
        _context->emit(object);
    }
}

void FilterProcessor::end_group() {
}

} // namespace python
} // namespace bigflow
} // namespace baidu
