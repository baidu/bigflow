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

#ifndef BIGFLOW_PYTHON_LIST_SERDE_H_
#define BIGFLOW_PYTHON_LIST_SERDE_H_

#include "boost/python.hpp"

#include <exception>

#include "glog/logging.h"

#include "bigflow_python/objectors/python_objector.h"
#include "bigflow_python/objectors/serde_impl_wrapper.h"
#include "bigflow_python/common/python.h"

#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class SameTypeListSerde : public PythonObjector {
    uint32_t serialize_list_element(void* element, char* buffer, uint32_t buffer_size);
    void* deserialize_list_element(const char* buffer, uint32_t buffer_size);
    void release_list_element(void *element);
public:
    SameTypeListSerde(const boost::python::object list_schema):
        _list_schema(list_schema) {
    }
    virtual ~SameTypeListSerde() {
        _serde_impl.release_all();
    }
    virtual void setup(const std::string& config);
    virtual uint32_t serialize(void* object, char* buffer, uint32_t buffer_size);
    virtual void* deserialize(const char* buffer, uint32_t buffer_size);
    virtual void release(void* object);
private:
    boost::python::object _list_schema;
    SerdeImplWrapper _serde_impl;
};

class TupleLikeListSerde : public PythonObjector {
public:
    TupleLikeListSerde(const boost::python::object list_schema):_list_schema(list_schema) {
    }
    virtual ~TupleLikeListSerde(){
        for (size_t i = 0; i < _serde_impls.size(); ++i) {
            _serde_impls[i].release_all();
        }
        _serde_impls.clear();
    }
    virtual void setup(const std::string& config);
    virtual uint32_t serialize(void* object, char* buffer, uint32_t buffer_size);
    virtual void* deserialize(const char* buffer, uint32_t buffer_size);
    virtual void release(void* object){
        delete static_cast<boost::python::object*>(object);
    }
private:
    std::vector<SerdeImplWrapper> _serde_impls;
    boost::python::object _list_schema;
};

class ListSerde : public PythonObjector {
private:
    boost::python::object _get_origin_serde(boost::python::object serde);
public:
    ListSerde():_delegate_objector(NULL){
    }
    virtual ~ListSerde(){
        if (_delegate_objector != NULL) {
            delete _delegate_objector;
            _delegate_objector = NULL;
        }
    }
public:
    virtual void setup(const std::string& config);
    virtual uint32_t serialize(void* object, char* buffer, uint32_t buffer_size) {
        CHECK(_delegate_objector != NULL);
        return _delegate_objector->serialize(object, buffer, buffer_size);
    }

    virtual void* deserialize(const char* buffer, uint32_t buffer_size) {
        CHECK(_delegate_objector != NULL);
        return _delegate_objector->deserialize(buffer, buffer_size);
    }

    virtual void release(void* object) {
        CHECK(_delegate_objector != NULL);
        return _delegate_objector->release(object);
    }
private:
    PythonObjector* _delegate_objector;
};

} // namespace python
} // namespace bigflow
} // namespace baidu

#endif  // BIGFLOW_PYTHON_LIST_SERDE_H_
