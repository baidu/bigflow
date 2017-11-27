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
// Author: Wang Cong <bigflow-opensource@baidu.com>
//
// A delegating class for python Partitioners.

#ifndef BLADE_BIGFLOW_PYTHON_PYTHON_PARTITIONER_DELEGATOR_H
#define BLADE_BIGFLOW_PYTHON_PYTHON_PARTITIONER_DELEGATOR_H

#include "boost/python.hpp"
#include "toft/base/scoped_ptr.h"

#include "bigflow_python/python_interpreter.h"

#include "flume/core/partitioner.h"

namespace baidu {
namespace bigflow {
namespace python {

class PythonPartitionerDelegator : public flume::core::Partitioner {
public:
    PythonPartitionerDelegator();
    virtual ~PythonPartitionerDelegator();

    virtual void Setup(const std::string& config);

    virtual uint32_t Partition(void* object, uint32_t partition_number);

private:
    bool _first_round;
    boost::python::object _partition;
    boost::python::tuple _args;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BLADE_BIGFLOW_PYTHON_PYTHON_PARTITIONER_DELEGATOR_H
