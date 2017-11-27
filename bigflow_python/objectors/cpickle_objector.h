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
// Author: Pan Yunhong (bigflow-opensource@baidu.com)
//

#ifndef BIGFLOW_PYTHON_OBJECTORS_CPICKLE_OBJECTOR_H_
#define BIGFLOW_PYTHON_OBJECTORS_CPICKLE_OBJECTOR_H_

#include "bigflow_python/objectors/python_objector.h"

#include "toft/base/scoped_ptr.h"

namespace baidu {
namespace bigflow {
namespace python {

class CPickleObjector : public PythonObjector {
public:
    class Impl;

    CPickleObjector();

    virtual void setup(const std::string& config);

    virtual uint32_t serialize(void* object, char* buffer, uint32_t buffer_size);

    virtual void* deserialize(const char* buffer, uint32_t buffer_size);

    virtual void release(void* object);

    virtual ~CPickleObjector();

private:
    toft::scoped_ptr<Impl> _impl;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif //  BIGFLOW_PYTHON_OBJECTORS_CPICKLE_OBJECTOR_H_
