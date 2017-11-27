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
// Author: Zhang Yuncong(bigflow-opensource@baidu.com)
// A delegating class for python Processors.

#include "sort_optimize_processor.h"

#include "bigflow_python/serde/cpickle_serde.h"
#include  "flume/core/objector.h"

namespace baidu {
namespace bigflow {
namespace python {

void SetValueNoneProcessor::Process(uint32_t index, void* object) {
    _emitter->Emit(&_none);
}

void SetKeyToValueProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
    using flume::core::Entity;
    using flume::core::Objector;
    std::string conf = boost::python::extract<std::string>(CPickleSerde().loads(config));
    baidu::flume::PbEntity entity;
    CHECK(entity.ParseFromArray(conf.data(), conf.size()))
            << "config size: " << conf.size() << "config value: " << conf.substr(0, 30);
    _objector.reset(Entity<Objector>::From(entity).CreateAndSetup());
}

void SetKeyToValueProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                 ProcessorContext* context) {
    toft::StringPiece key = keys.back();
    void* key_ptr = _objector->Deserialize(key.data(), key.size());
    CHECK_NOTNULL(key_ptr);
    _key = *static_cast<boost::python::object*>(key_ptr);
    _objector->Release(key_ptr);
    _context = context;
}

void SetKeyToValueProcessor::process(void* object) {
    _context->emit(&_key);
}

void SetKeyToValueProcessor::end_group() {}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu
