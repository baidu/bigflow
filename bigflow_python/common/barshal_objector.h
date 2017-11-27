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
// Author: PanYunhong <bigflow-opensource@baidu.com>
//
#ifndef BIGFLOW_PYTHON_BARSHAL_OBJECTOR_H
#define BIGFLOW_PYTHON_BARSHAL_OBJECTOR_H

#include "flume/core/objector.h"
#include "boost/python.hpp"
#include "marshal.h"
#include "glog/logging.h"

namespace baidu {
namespace bigflow {
namespace python {

// Modified version of marshal Objector, if an object can be serialized by this objector,
// the serialized result could be deserialized by marshal.
// There are two big differences between BarshalObjector and marshal:
// 1. BarshalObjector guarantees the same object will be serialized to the same str,
//    while marshal don't have this guarantee. Eg.
//      marshal.dumps('a') == 't\x01\x00\x00\x00a'
//      marshal.dumps('a'.lower()) == 's\x01\x00\x00\x00a'
//    This guarantee is important if you want to use the serialized data as a key of shuffling
//    and partitioning.
// 2. BarshalObjector supports bigflow SideInput, if a SideInput object is found,
//    it will be converted to a list before being serialized.

class BarshalObjector : public flume::core::Objector {
    virtual void Setup(const std::string& config){}

    // save key to buffer if has enough buffer
    // return key size
    virtual uint32_t Serialize(void* object, char* buffer, uint32_t buffer_size);

    // return an address to deserialized object.
    virtual void* Deserialize(const char* buffer, uint32_t buffer_size);

    // release resources hold by object
    virtual void Release(void* object);
};

} // namespace python
} // namespace bigflow
} // namespace baidu
#endif /* !BIGFLOW_PYTHON_BARSHAL_H */
