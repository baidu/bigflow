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
/**
* @created:     2015/06/18
* @filename:    python_impl_functor.h
* @author:      zhangyuncong (bigflow-opensource@baidu.com)
*/

#ifndef BIGFLOW_PYTHON_SERDE_WRAPPER_FN_H
#define BIGFLOW_PYTHON_SERDE_WRAPPER_FN_H

#include "boost/python.hpp"
#include "glog/logging.h"

#include "bigflow_python/functors/functor.h"

#include "bigflow_python/serde/cpickle_serde.h"

#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class SerdeWrapperFn : public Functor {
private:
    void apply_tuple_item(const boost::python::object& apply_obj, flume::core::Emitter* emitter);
    void apply_whole_object(const boost::python::object& apply_obj, flume::core::Emitter* emitter);
public:
    SerdeWrapperFn();
    virtual void Setup(const std::string& config);
    virtual void call(void* object, flume::core::Emitter* emitter);
    virtual ~SerdeWrapperFn();
private:
    bool _is_serialize;
    std::string _objector_name;
    std::string _objector_config;
    flume::core::Objector* _objector;
    const static uint32_t INIT_BUFFER_SIZE = 64;
    char* _buffer;
    uint32_t _buffer_size;
    int64_t _apply_index;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_PYTHON_SERDE_WRAPPER_FN_H
