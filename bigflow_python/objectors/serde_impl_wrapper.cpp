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

#include "bigflow_python/objectors/serde_impl_wrapper.h"

namespace baidu {
namespace bigflow {
namespace python {

SerdeImplWrapper create_serde_impl_wrapper(const boost::python::object& item) {

    SerdeImplWrapper serde_element;

    boost::python::object entity_names =
            boost::python::import("bigflow.core.entity_names");
    boost::python::object entity_dict = entity_names.attr("__dict__");

    boost::python::object entity_name = item.attr("get_entity_name")();
    boost::python::object entity_config = item.attr("get_entity_config")();
    std::string str_config = boost::python::extract<std::string>(entity_config);

    std::string name_key = boost::python::extract<std::string>(entity_name);
    std::string name_value = \
            boost::python::extract<std::string>(entity_dict[name_key]);

    flume::core::Entity<flume::core::Objector> entity(name_value, str_config);
    flume::core::Objector* objector = entity.CreateAndSetup();
    serde_element.set_serde_objector(objector);

    return serde_element;
}

} // namespace python
} // namespace bigflow
} // namespace baidu
