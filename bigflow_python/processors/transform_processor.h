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
* @filename:    transform_processor.h
* @author:      bigflow-opensource@baidu.com
* @brief:       transform processor implementation
*/

#ifndef BIGFLOW_PYTHON_TRANSFORM_PROCESSOR_H_
#define BIGFLOW_PYTHON_TRANSFORM_PROCESSOR_H_

#include "boost/python.hpp"

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "bigflow_python/delegators/python_processor_delegator.h"
#include "bigflow_python/functors/functor.h"
#include "bigflow_python/functors/py_functor_caller.h"
#include "bigflow_python/processors/processor.h"

namespace baidu {

namespace flume {
namespace core {
class Objector;
}
}

namespace bigflow {
namespace python {

class TransformProcessor : public Processor {
public:
    TransformProcessor();
    virtual ~TransformProcessor();

public:
    virtual void setup(const std::vector<Functor*>& fns, const std::string& config);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys,
                             ProcessorContext* context);

    virtual void process(void* object);

    virtual void end_group();

    virtual uint32_t serialize(char* buffer, uint32_t buffer_size);

    virtual bool deserialize(const char* buffer, uint32_t buffer_size);

private:
    toft::scoped_ptr<PyFunctorCaller> _initializer_fn;
    toft::scoped_ptr<PyFunctorCaller> _transformer_fn;
    toft::scoped_ptr<PyFunctorCaller> _finalizer_fn;
    toft::scoped_ptr<toft::Closure<void ()> > _done_fn;
    toft::scoped_ptr<flume::core::Objector> _objector;
    EmitterDelegator _emitter_delegator;
    bool _already_new_tuple;
    boost::python::tuple _init_args;
    boost::python::tuple _args;
    boost::python::tuple _final_args;
    boost::python::object _emitter;
    ProcessorContext* _context;
};

} // namespace python
} // namespace bigflow
} // namespace baidu

#endif
