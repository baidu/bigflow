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
// Author: Wang Cong <wangcong09@baidu.com>
//
// A delegating class for python KeyReaders.

#include "python_partitioner_delegator.h"

#include "glog/logging.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

void PythonPartitionerDelegator::Setup(const std::string& config) {
    try {
        boost::python::object partitioner = CPickleSerde().loads(config);
        _partition = partitioner.attr("partition");
        _args = new_tuple(2);
        _first_round = true;
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

uint32_t PythonPartitionerDelegator::Partition(void* object, uint32_t partition_number) {
    try {

        if (_first_round) {
            tuple_set_item(_args, 1, boost::python::object(partition_number));
            _first_round = false;
        }

        tuple_set_item(_args, 0, *static_cast<boost::python::object*>(object));
        uint32_t ret = boost::python::extract<uint32_t>(_partition(*_args));
        return ret;
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    } catch (std::exception &e) {
        LOG(FATAL) << "Exception: " << e.what()
                   << "Calculating partition number error!!!"
                   << "Partition number calculated: "
                   << boost::python::extract<std::string>(boost::python::str(_partition(*_args)))()
                   << "Partition Key: "
                   << boost::python::extract<std::string>(tuple_get_item(_args, 0))();
    } catch (...) {
        LOG(FATAL) << "Calculating partition number error!!!,"
            " but I have no idea what happened!";
    }
}

PythonPartitionerDelegator::PythonPartitionerDelegator() {}

PythonPartitionerDelegator::~PythonPartitionerDelegator() {}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

