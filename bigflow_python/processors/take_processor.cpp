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
* @filename:    take_processor.cpp
* @author:      fangjun02@baidu.com
* @brief:       take processor implementation
*/

#include "bigflow_python/processors/take_processor.h"

#include <sstream>

#include "glog/logging.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/proto/processor.pb.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/functors/functor.h"
#include "bigflow_python/serde/cpickle_serde.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

void TakeProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
    _has_side_input = true;
    if (!config.empty()) {
        _has_side_input = false;
        boost::python::object limit = CPickleSerde().loads(config);
        _num = boost::python::extract<int64_t>(limit);
    }
}

void TakeProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                ProcessorContext* context) {
    _context = context;
    const std::vector<boost::python::object>& side_input_list = context->python_side_inputs();
    if (_has_side_input) {
        _num = boost::python::extract<int64_t>(side_input_list[0]);
    }
    _count = 0;
}

void TakeProcessor::process(void* object) {
    if (++_count <= _num) {
        bool has_more_output = _context->emit(object);
        if ((!has_more_output) || (_count == _num)) {
            _context->done();
        }
    } else {
        _context->done();
    }
}

void TakeProcessor::end_group() {
}

} // namespace python
} // namespace bigflow
} // namespace baidu
