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
* @created:     2015/06/19
* @filename:    flatten_processor.cpp
* @author:      zhangyuncong@baidu.com
*/

#include "bigflow_python/processors/flatten_processor.h"

#include "glog/logging.h"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/proto/entity.pb.h"

#include "bigflow_python/functors/py_functor_caller.h"
#include "bigflow_python/proto/processor.pb.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

void FlattenProcessor::setup(const std::vector<Functor*>& fn, const std::string& config) {
    using flume::core::Entity;
    using flume::core::Objector;
    std::string conf = boost::python::extract<std::string>(CPickleSerde().loads(config));
    baidu::flume::PbEntity entity;
    CHECK(entity.ParseFromArray(conf.data(), conf.size()))
            << "config size: " << conf.size() << "config value: " << conf.substr(0, 30);
    _objector.reset(Entity<Objector>::From(entity).CreateAndSetup());
}

void FlattenProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                   ProcessorContext* context) {
    using boost::python::object;
    _context = context;
    toft::StringPiece key = keys.back();
    object* kobj = static_cast<object*>(_objector->Deserialize(key.data(), key.size()));
    _key = *kobj;
    _objector->Release(kobj);
}

void FlattenProcessor::process(void* object) {
    boost::python::object input = *static_cast<boost::python::object*>(object);
    boost::python::tuple tuple = boost::python::make_tuple(_key, input);
    _context->emit(&tuple);
}

void FlattenProcessor::end_group() {
}

FlattenProcessor::~FlattenProcessor() {
}

} // namespace python
} // namespace bigflow
} // namespace baidu
