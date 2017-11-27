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
* @created:     2015/12/19
* @filename:    pipe_processor.h
* @author:      xuyao02@baidu.com
* @brief:       pipe processor implementation
*/

#ifndef BIGFLOW_PYTHON_PIPE_PROCESSOR_H
#define BIGFLOW_PYTHON_PIPE_PROCESSOR_H

#include "boost/python.hpp"

#include <string>
#include <vector>


#include "toft/base/scoped_ptr.h"

#include "bigflow_python/functors/functor.h"
#include "bigflow_python/functors/py_functor_caller.h"
#include "bigflow_python/processors/processor.h"

namespace baidu {
namespace bigflow {
namespace python {

class PipeProcessor : public Processor {
public:
    PipeProcessor();
    virtual ~PipeProcessor();

public:
    virtual void setup(const std::vector<Functor*>& fns, const std::string& config);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys,
                             ProcessorContext* context);

    virtual void process(void* object);

    virtual void end_group();

private:
    class Impl;
    toft::scoped_ptr<Impl> _impl;
};

} // namespace python
} // namespace bigflow
} // namespace baidu

#endif // BIGFLOW_PYTHON_PIPE_PROCESSOR_H
