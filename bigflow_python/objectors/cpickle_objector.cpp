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

#include "bigflow_python/objectors/cpickle_objector.h"

namespace baidu {
namespace bigflow {
namespace python {

class CPickleObjector::Impl {
public:
    virtual void setup(const std::string& config){
        _pickle = boost::python::import("cPickle");
        _loads = _pickle.attr("loads");
        _dumps = _pickle.attr("dumps");
    }

    virtual uint32_t serialize(void* object, char* buffer, uint32_t buffer_size){
        boost::python::object* input_obj = static_cast<boost::python::object*>(object);
        boost::python::object result = _dumps(*input_obj);
        char* buf = NULL;
        Py_ssize_t size = 0;
        PyString_AsStringAndSize(result.ptr(), &buf, &size);
        if (size <= buffer_size) {
            memcpy(buffer, buf, size);
        }
        return size;
    }

    virtual void* deserialize(const char* buffer, uint32_t buffer_size){
        return new boost::python::object(_loads(boost::python::str(buffer, buffer_size)));
    }

    virtual void release(void* object){
        delete static_cast<boost::python::object*>(object);
    }

    ~Impl() {}

private:
    boost::python::object _pickle;
    boost::python::object _loads;
    boost::python::object _dumps;
};

CPickleObjector::CPickleObjector() {
    _impl.reset(new Impl);
}

void CPickleObjector::setup(const std::string& config) {
    return _impl->setup(config);
}

uint32_t CPickleObjector::serialize(void* object, char* buffer, uint32_t buffer_size) {
    return _impl->serialize(object, buffer, buffer_size);
}

void* CPickleObjector::deserialize(const char* buffer, uint32_t buffer_size) {
    return _impl->deserialize(buffer, buffer_size);
}

void CPickleObjector::release(void* object) {
    return _impl->release(object);
}

CPickleObjector::~CPickleObjector() {}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

