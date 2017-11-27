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
// Author: Zhang Xiaohu <zhangxiaohu02@baidu.com>
//

#ifndef BIGFLOW_PYTHON_CHAIN_SERDE_H
#define BIGFLOW_PYTHON_CHAIN_SERDE_H

#include "boost/python.hpp"

#include <exception>

#include "glog/logging.h"

#include "bigflow_python/objectors/python_objector.h"
#include "bigflow_python/objectors/serde_impl_wrapper.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

class ChainSerde : public PythonObjector {
public:
    ChainSerde() {}
    virtual ~ChainSerde() {
        for (size_t i = 0; i < _chain_element_serde.size(); ++i) {
            _chain_element_serde[i].release_all();
        }
        _chain_element_serde.clear();
    }

    virtual void setup(const std::string& config);
    virtual uint32_t serialize(void* object, char* buffer, uint32_t buffer_size);

    virtual void* deserialize(const char* buffer, uint32_t buffer_size);

    virtual void release(void* object);

private:
    std::vector<SerdeImplWrapper> _chain_element_serde;
};

} // namespace python
} // namespace bigflow
} // namespace baidu

#endif // BIGFLOW_PYTHON_CHAIN_SERDE_H
