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

#include "bigflow_python/objectors/dict_serde.h"

#include <iostream>

#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

void DictSerde::setup(const std::string& config) {
    boost::python::object real_dict_obj = get_origin_serde(CPickleSerde().loads(config));
    _dict_key_type = real_dict_obj.attr("_key");
    _dict_val_type = real_dict_obj.attr("_value");
    _tp_serde = real_dict_obj.attr("_tp_serde");
    std::string reflect_str = get_entity_reflect_str(_tp_serde);
    std::string tp_serde_config = get_entity_config_str(_tp_serde);
    flume::core::Entity<flume::core::Objector> entity(reflect_str, tp_serde_config);
    _serde = entity.CreateAndSetup();
    CHECK(_serde != NULL);
}

uint32_t DictSerde::serialize(void* object, char* buffer, uint32_t buffer_size) {
    boost::python::dict* dict_obj = static_cast<boost::python::dict*>(object);
    boost::python::list dict_items = dict_obj->items();
    return _serde->Serialize(&dict_items, buffer, buffer_size);
}

void* DictSerde::deserialize(const char* buffer, uint32_t buffer_size) {
    boost::python::object* deserialized_obj = \
        static_cast<boost::python::object*>(_serde->Deserialize(buffer, buffer_size));
    boost::python::dict* dict_obj = new boost::python::dict(*deserialized_obj);
    _serde->Release(deserialized_obj);
    return dict_obj;
}

void DictSerde::release(void* object) {
    delete static_cast<boost::python::dict*>(object);
}

} // namespace python
} // namespace bigflow
} // namespace baidu
