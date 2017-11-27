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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>
//
// Accumulate Processor

#include "bigflow_python/processors/combine_processor.h"

#include <exception>

#include "glog/logging.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/python_args.h"
#include "bigflow_python/functors/functor.h"

namespace baidu {
namespace bigflow {
namespace python {

void CombineProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
    _fn.reset(new PyFunctorCaller(fns[0]));
    _args = new_tuple(1);
}

void CombineProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                      ProcessorContext* context) {
    const std::vector<boost::python::object>& vec = context->python_side_inputs();
    tuple_set_item(_args, 0, vec[0]);
    (*_fn)(context->emitter(), &_args);
}

void CombineProcessor::process(void*) {
}

void CombineProcessor::end_group() {
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

