/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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
// A delegating class for python Processors.

#ifndef BLADE_BIGFLOW_PYTHON_SORT_OPTIMIZE_PROCESSOR_H
#define BLADE_BIGFLOW_PYTHON_SORT_OPTIMIZE_PROCESSOR_H

#include "bigflow_python/delegators/python_processor_delegator.h"

#include <exception>

#include "bigflow_python/processors/processor.h"

namespace baidu {

namespace flume {
namespace core {
class Objector;
}
}

namespace bigflow {
namespace python {

class SetValueNoneProcessor : public PythonIOProcessorBase {
public:
    virtual void Process(uint32_t index, void* object);

protected:
    boost::python::object _none;
};

class SetKeyToValueProcessor : public Processor {
public:

    virtual void setup(const std::vector<Functor*>& fns, const std::string& config);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys,
                             ProcessorContext* context);

    virtual void process(void* object);
    virtual void end_group();

private:
    toft::scoped_ptr<flume::core::Objector> _objector;
    boost::python::object _key;
    ProcessorContext* _context;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BLADE_BIGFLOW_PYTHON_SORT_OPTIMIZE_PROCESSOR_H
