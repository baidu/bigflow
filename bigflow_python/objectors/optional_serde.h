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
// Author: Pan Yunhong <panyunhong@baidu.com>
//

#ifndef BIGFLOW_PYTHON_OPTIONAL_SERDE_H_
#define BIGFLOW_PYTHON_OPTIONAL_SERDE_H_

#include "boost/python.hpp"

#include <exception>

#include "boost/ptr_container/ptr_vector.hpp"
#include "glog/logging.h"

#include "bigflow_python/objectors/python_objector.h"
#include "bigflow_python/objectors/serde_impl_wrapper.h"

#include "bigflow_python/serde/cpickle_serde.h"

#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class OptionalSerde : public PythonObjector {
public:
    OptionalSerde(){}
    virtual ~OptionalSerde(){
        _serde_impl.release_all();
    }
public:
    virtual void setup(const std::string& config);
    virtual uint32_t serialize(void* object, char* buffer, uint32_t buffer_size);
    virtual void* deserialize(const char* buffer, uint32_t buffer_size);
    virtual void release(void* object);
private:
    inline boost::python::object get_origin_serde_obj(const std::string& config) {
        return CPickleSerde().loads(config).attr("origin_serde")();
    }
private:
    SerdeImplWrapper _serde_impl;
};

} // namespace python
} // namespace bigflow
} // namespace baidu

#endif  // BIGFLOW_PYTHON_OPTIONAL_SERDE_H_
