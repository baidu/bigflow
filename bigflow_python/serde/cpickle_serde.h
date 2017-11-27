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
// Author: Zhang Yuncong (zhangyuncong@baidu.com)
//

#ifndef BIGFLOW_PYTHON_SERDE_CPICKLE_SERDE_H_
#define BIGFLOW_PYTHON_SERDE_CPICKLE_SERDE_H_

#include "bigflow_python/serde.h"

#include "toft/base/scoped_ptr.h"

namespace baidu {
namespace bigflow {
namespace python {

class CPickleSerde : public baidu::bigflow::python::Serde{
public:
    CPickleSerde(bool python_interpreter_already_inited);
    CPickleSerde(); // same as CPickleSerde(false)

    virtual boost::python::object loads(const std::string& py_serialized) const;
    virtual boost::python::object loads(const char * py_str, uint32_t size) const;
    virtual boost::python::object dumps(boost::python::object& py_obj) const;

    virtual ~CPickleSerde();

private:
    class Impl;
    toft::scoped_ptr<Impl> _impl;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif //  BIGFLOW_PYTHON_SERDE_CPICKLE_SERDE_H_
