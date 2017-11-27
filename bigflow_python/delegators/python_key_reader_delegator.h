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

#ifndef BIGFLOW_PYTHON_PYTHON_KEY_READER_DELEGATOR_H_
#define BIGFLOW_PYTHON_PYTHON_KEY_READER_DELEGATOR_H_

#include "boost/python.hpp"
#include "toft/base/scoped_ptr.h"

#include "bigflow_python/python_interpreter.h"

#include "flume/core/key_reader.h"

namespace baidu {
namespace flume {
namespace core {

class Objector;

} // namespace core
} // namespace flume

namespace bigflow {
namespace python {

class PythonKeyReaderDelegator : public flume::core::KeyReader {
public:
    PythonKeyReaderDelegator();
    virtual ~PythonKeyReaderDelegator();

    virtual void Setup(const std::string& config);

    virtual uint32_t ReadKey(void* object, char* buffer, uint32_t buffer_size);

private:
    class ReadKeyFn;
    toft::scoped_ptr<ReadKeyFn> _read_key_fn;
    toft::scoped_ptr<flume::core::Objector> _objector;
    bool _enable_serialize;
};

class StrKeyReaderDelegator : public flume::core::KeyReader {
public:
    StrKeyReaderDelegator();
    virtual ~StrKeyReaderDelegator();

    virtual void Setup(const std::string& config);

    virtual uint32_t ReadKey(void* object, char* buffer, uint32_t buffer_size);

private:
    class ReadKeyFn;
    toft::scoped_ptr<ReadKeyFn> _read_key_fn;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_PYTHON_PYTHON_KEY_READER_DELEGATOR_H_
