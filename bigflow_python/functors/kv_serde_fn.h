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
/**
* @created:     2016/03/10
* @filename:    kv_serde_fn.h
* @author:      wenchunyang (wenchunyang@baidu.com)
*/

#ifndef BIGFLOW_PYTHON_KV_SERDE_FN_H
#define BIGFLOW_PYTHON_KV_SERDE_FN_H

#include <vector>

#include "boost/python.hpp"
#include "glog/logging.h"

#include "bigflow_python/objectors/serde_impl_wrapper.h"
#include "bigflow_python/functors/functor.h"

#include "bigflow_python/serde/cpickle_serde.h"

#include "flume/core/objector.h"
#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class KVSerdeFn : public Functor {
protected:
    void apply_kv_object(const boost::python::object& apply_obj, flume::core::Emitter* emitter);
    virtual void apply_single_serde(SerdeImplWrapper& serde,
            const boost::python::object& apply_obj,
            int data_ind) {};

    // those member need to be protected
    // derived class maybe need to accessthem
    std::vector<SerdeImplWrapper> _objectors;
    boost::python::tuple _tuple;
    const static uint32_t INIT_BUFFER_SIZE = 64;
    char* _buffer;
    uint32_t _buffer_size;
public:
    KVSerdeFn();
    virtual void Setup(const std::string& config);
    virtual void call(void* object, flume::core::Emitter* emitter);
    virtual ~KVSerdeFn();

};

class KVSerializeFn : public KVSerdeFn {
protected:
    virtual void apply_single_serde(SerdeImplWrapper& serde,
            const boost::python::object& apply_obj,
            int data_ind);
};

class KVDeserializeFn : public KVSerdeFn {
protected:
    virtual void apply_single_serde(SerdeImplWrapper& serde,
            const boost::python::object& apply_obj,
            int data_ind);
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_PYTHON_KV_SERDE_FN_H

