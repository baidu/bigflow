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
* @created:     2015/07/03
* @filename:    get_last_key_processor.cpp
* @author:      bigflow-opensource@baidu.com
* @brief:       get last key processor implementation
*/

#include "bigflow_python/processors/get_last_key_processor.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

void GetLastKeyProcessor::setup(const std::vector<Functor*>& fns,
                                const std::string& config) {
    CHECK_EQ(1u, fns.size());
    _deserialize.reset(new PyFunctorCaller(fns[0]));
    _args = new_tuple(1);
}

void GetLastKeyProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                      ProcessorContext* context) {
    _done_fn.reset(toft::NewPermanentClosure(context, &ProcessorContext::done));
    CHECK(!keys.empty());
    toft::StringPiece key = keys.back();
    tuple_set_item(_args, 0, boost::python::str(key.data(), key.size()));
    _key = (*_deserialize)(_done_fn.get(), &_args);
    context->emit(&_key);
    context->done();
}

void GetLastKeyProcessor::process(void* object) {
}

void GetLastKeyProcessor::end_group() {
}

} // namespace python
} // namespace bigflow
} // namespace baidu
