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
* @filename:    flatmap_processor.cpp
* @author:      fangjun02@baidu.com
* @brief:       flatmap processor implementation
*/

#include "bigflow_python/processors/flatmap_processor.h"

#include "glog/logging.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/proto/processor.pb.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/functors/functor.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

void FlatMapProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
    _fn.reset(new PyFunctorCaller(fns[0]));
    _already_new_tuple = false;
}

void FlatMapProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                   ProcessorContext* context) {
    _context = context;
    const std::vector<boost::python::object>& side_inputs = context->python_side_inputs();

    int len = side_inputs.size();
    if (!_already_new_tuple) {
        _normal_args = new_tuple(len + 1);
        _already_new_tuple = true;
    }
    for (int i = 0; i != len; ++i) {
        tuple_set_item(_normal_args, i + 1, side_inputs[i]);
    }
}

void FlatMapProcessor::process(void* object) {
    tuple_set_item(_normal_args, 0, *static_cast<boost::python::object*>(object));
    (*_fn)(_context->emitter(), &_normal_args);
}

void FlatMapProcessor::end_group() {
}

} // namespace python
} // namespace bigflow
} // namespace baidu
