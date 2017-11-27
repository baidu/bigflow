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
// Processors used by transforms.count

#include "bigflow_python/processors/count_processors.h"

#include "bigflow_python/common/python.h"

namespace baidu {
namespace bigflow {
namespace python {

void CountProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
}

void CountProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                      ProcessorContext* context) {
    _context = context;
    _cnt = 0;
}

void CountProcessor::process(void*) {
    ++_cnt;
}

void CountProcessor::end_group() {
    _context->emit(&_cnt);
}

void SumProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
}

void SumProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                      ProcessorContext* context) {
    _context = context;
    _sum = 0;
}

void SumProcessor::process(void* cnt) {
    _sum += *static_cast<uint64_t*>(cnt);
}

void SumProcessor::end_group() {
    boost::python::object sum = boost::python::long_(_sum);
    _context->emit(&sum);
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

