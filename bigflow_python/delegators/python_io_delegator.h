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
// A delegating class for python Processors.

#ifndef BLADE_BIGFLOW_PYTHON_PYTHON_IO_DELEGATOR_H
#define BLADE_BIGFLOW_PYTHON_PYTHON_IO_DELEGATOR_H

#include "toft/base/scoped_ptr.h"

#include "flume/core/loader.h"
#include "flume/core/sinker.h"
#include "flume/core/objector.h"

namespace baidu {
namespace bigflow {
namespace python {

class PythonLoaderDelegator : public flume::core::Loader {
public:
    PythonLoaderDelegator();

    virtual void Setup(const std::string& config);

    virtual void Split(const std::string& uri, std::vector<std::string>* splits);

    virtual void Load(const std::string& split, flume::core::Emitter* emitter);

private:
    toft::scoped_ptr<flume::core::Loader> _impl;
};

class PythonSinkerDelegator : public flume::core::Sinker {
public:
    PythonSinkerDelegator();

    virtual void Setup(const std::string& config);

    virtual void Open(const std::vector<toft::StringPiece>& keys);

    virtual void Sink(void* object);

    virtual void Close();

private:
    toft::scoped_ptr<flume::core::Sinker> _impl;
};

class EncodePartitionObjector : public flume::core::Objector {
public:

    virtual void Setup(const std::string& config);

    // do not support this method for now.
    virtual uint32_t Serialize(void* object, char* buffer, uint32_t buffer_size);

    virtual void* Deserialize(const char* buffer, uint32_t buffer_size);

    // release resources hold by object
    virtual void Release(void* object);
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BLADE_BIGFLOW_PYTHON_PYTHON_IO_DELEGATOR_H
