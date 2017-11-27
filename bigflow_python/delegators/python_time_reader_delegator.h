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
// Author: bigflow-opensource@baidu.com
//
// A delegating class for python time readers.

#ifndef BIGFLOW_PYTHON_PYTHON_TIME_READER_DELEGATOR_H_
#define BIGFLOW_PYTHON_PYTHON_TIME_READER_DELEGATOR_H_

#include "boost/python.hpp"

#include "toft/base/scoped_ptr.h"

#include "flume/core/time_reader.h"

namespace baidu {
namespace bigflow {
namespace python {

class PythonTimeReaderDelegator : public flume::core::TimeReader {
public:
    PythonTimeReaderDelegator();
    virtual ~PythonTimeReaderDelegator();

    virtual void Setup(const std::string& config);

    virtual uint64_t get_timestamp(void* object);

protected:
    class Impl;
    toft::scoped_ptr<Impl> _impl;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_PYTHON_PYTHON_TIME_READER_DELEGATOR_H_
