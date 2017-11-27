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

#ifndef BIGFLOW_PYTHON_FIELDS_SERDE_H_
#define BIGFLOW_PYTHON_FIELDS_SERDE_H_

#include <exception>

#include "boost/python.hpp"
#include "glog/logging.h"

#include "bigflow_python/objectors/python_objector.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"

#include "flume/core/entity.h"

namespace baidu {
namespace bigflow {
namespace python {

class FieldsSerde : public PythonObjector {
public:
    FieldsSerde(){}
    virtual ~FieldsSerde(){}
public:
    virtual void setup(const std::string& config){
        boost::python::object fields_serde_obj = CPickleSerde().loads(config);
        boost::python::object entity_names_dict =
            boost::python::import("bigflow.core.entity_names").attr("__dict__");

        _fields = fields_serde_obj.attr("_fields");
        _fields_len = list_len(_fields);
        boost::python::object tuple_serde_obj = fields_serde_obj.attr("_tuple_serde");
        boost::python::object entity_name = tuple_serde_obj.attr("get_entity_name")();
        boost::python::object entity_config = tuple_serde_obj.attr("get_entity_config")();
        std::string name_key = boost::python::extract<std::string>(entity_name);
        std::string str_config = boost::python::extract<std::string>(entity_config);
        std::string name_value =
            boost::python::extract<std::string>(entity_names_dict[name_key]);

        flume::core::Entity<flume::core::Objector> entity(name_value, str_config);
        _tuple_serde.reset(entity.CreateAndSetup());
    }

    virtual uint32_t serialize(void* object, char* buffer, uint32_t buffer_size){
        boost::python::dict* obj_dict = static_cast<boost::python::dict*>(object);
        boost::python::tuple fields_tuple = new_tuple(_fields_len);
        for (int64_t i = 0; i < _fields_len; ++i) {
            boost::python::object list_item = list_get_item(_fields, i);
            tuple_set_item(fields_tuple, i, obj_dict->get(list_item, boost::python::object()));
        }
        return _tuple_serde->Serialize(&fields_tuple, buffer, buffer_size);
    }

    virtual void* deserialize(const char* buffer, uint32_t buffer_size){
        CHECK_GT(buffer_size, 0u);
        boost::python::tuple* result_tuple =
            static_cast<boost::python::tuple*>(_tuple_serde->Deserialize(buffer, buffer_size));

        boost::python::dict *result_dict = new boost::python::dict();
        for (int64_t i = 0; i < _fields_len; ++i) {
            boost::python::object key = list_get_item(_fields, i);
            (*result_dict)[key] = tuple_get_item(*result_tuple, i);
        }
        _tuple_serde->Release(result_tuple);
        return result_dict;
    }

    virtual void release(void* object) {
        delete static_cast<boost::python::dict*>(object);
    }
private:
    toft::scoped_ptr<flume::core::Objector> _tuple_serde;
    boost::python::object _fields;
    int64_t _fields_len;
};

} // namespace python
} // namespace bigflow
} // namespace baidu

#endif  // BIGFLOW_PYTHON_FIELDS_SERDE_H_
