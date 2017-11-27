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
* @filename:    select_elements_processor.h
* @author:      fangjun02@baidu.com
* @brief:       select elements processor implementation
*/

#ifndef BIGFLOW_PYTHON_SELECT_ELEMENTS_PROCESSOR_H_
#define BIGFLOW_PYTHON_SELECT_ELEMENTS_PROCESSOR_H_

#include "boost/python.hpp"

#include <algorithm>
#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "bigflow_python/functors/functor.h"
#include "bigflow_python/functors/py_functor_caller.h"
#include "bigflow_python/processors/processor.h"

namespace baidu {
namespace bigflow {
namespace python {

class SelectElementsProcessor : public Processor {
public:
    SelectElementsProcessor() {}
    virtual ~SelectElementsProcessor() {}

public:
    virtual void setup(const std::vector<Functor*>& fns, const std::string& config);

    virtual void begin_group(const std::vector<toft::StringPiece>& keys,
                             ProcessorContext* context);

    virtual void process(void* object);

    virtual void end_group();

public:
    class Compare {
    public:
        Compare(bool order) : _order(order) {}
        bool operator() (const boost::python::tuple& ob1,
                         const boost::python::tuple& ob2) {
            if (_order) {
                return ob1[0] > ob2[0];
            } else {
                return ob1[0] < ob2[0];
            }
        }

    private:
        bool _order;
    };

private:
    toft::scoped_ptr<PyFunctorCaller> _key_fn;
    toft::scoped_ptr<toft::Closure<void ()> > _done_fn;
    ProcessorContext* _context;
    int64_t _num;
    bool _order;
    toft::scoped_ptr<Compare> _compare;
    boost::python::tuple _tuple;
    std::vector<boost::python::tuple> _objects;
};

} // namespace python
} // namespace bigflow
} // namespace baidu

#endif
