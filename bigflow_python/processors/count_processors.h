/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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

#ifndef BIGFLOW_PYTHON_COUNT_PROCESSORS_H_
#define BIGFLOW_PYTHON_COUNT_PROCESSORS_H_

#include <stdint.h>

#include "bigflow_python/processors/processor.h"


namespace baidu {
namespace bigflow {
namespace python {

class CountProcessor : public Processor {
public:
    virtual void setup(const std::vector<Functor*>& fns, const std::string& config);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys,
                             ProcessorContext* context);

    virtual void process(void* object);

    virtual void end_group();

private:
    ProcessorContext* _context;
    uint64_t _cnt;
};

// This class can only be used in transforms.count
// Because its input type is a cpp type uint64_t, and its output type is a python type long
// Don't use it to sum other objects unless you know exactly what you are doing.
class SumProcessor : public Processor {
public:
    virtual void setup(const std::vector<Functor*>& fns, const std::string& config);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys,
                             ProcessorContext* context);

    virtual void process(void* object);

    virtual void end_group();

private:
    ProcessorContext* _context;
    uint64_t _sum;
};


}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif //BIGFLOW_PYTHON_COUNT_PROCESSORS_H_
