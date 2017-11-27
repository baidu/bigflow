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
// Author: Pan Yunhong <bigflow-opensource@baidu.com>
//

#ifndef BIGFLOW_PYTHON_SERDE_IMPL_WRAPPER_H_
#define BIGFLOW_PYTHON_SERDE_IMPL_WRAPPER_H_

#include "boost/python.hpp"

#include "glog/logging.h"

#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class SerdeImplWrapper {
public:
    SerdeImplWrapper() {
        _serde = NULL;
    }
    ~SerdeImplWrapper() {
    }
    inline void set_serde_objector(flume::core::Objector* objector) {
        _serde = objector;
    }
    uint32_t serialize(void* object, char* buffer, uint32_t buffer_size) {
        CHECK(_serde != NULL) <<
                "implementation object must not be NULL";
        return _serde->Serialize(object, buffer, buffer_size);
    }

    void* deserialize(const char* buffer, uint32_t buffer_size) {
        return _serde->Deserialize(buffer, buffer_size);
    }

    void release_object(void* object) {
        _serde->Release(object);
    }

    void release_all() {
        if (_serde != NULL) {
            delete _serde;
            _serde = NULL;
        }
    }
private:
    flume::core::Objector* _serde;
};

SerdeImplWrapper create_serde_impl_wrapper(const boost::python::object& item);

} // namespace python
} // namespace bigflow
} // namespace baidu

#endif  // BIGFLOW_PYTHON_SERDE_IMPL_WRAPPER_H_
