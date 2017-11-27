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
* @created:     2015/06/22
* @filename:    transform_processor.cpp
* @author:      bigflow-opensource@baidu.com
* @brief:       transform processor implementation
*/

#include "bigflow_python/processors/transform_processor.h"

#include "glog/logging.h"

#include "bigflow_python/proto/processor.pb.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"
#include "bigflow_python/functors/functor.h"
#include "flume/core/entity.h"
#include "flume/core/objector.h"

namespace baidu {
namespace bigflow {
namespace python {

TransformProcessor::TransformProcessor() {}
TransformProcessor::~TransformProcessor() {}

void TransformProcessor::setup(const std::vector<Functor*>& fns, const std::string& config) {
    using flume::core::Entity;
    using flume::core::Objector;
    try {
        _initializer_fn.reset(new PyFunctorCaller(fns[0]));
        _transformer_fn.reset(new PyFunctorCaller(fns[1]));
        _finalizer_fn.reset(new PyFunctorCaller(fns[2]));
        std::string conf = boost::python::extract<std::string>(CPickleSerde().loads(config));
        baidu::flume::PbEntity entity;
        CHECK(entity.ParseFromArray(conf.data(), conf.size()))
                << "config size: " << conf.size() << "config value: " << conf.substr(0, 30);
        _objector.reset(Entity<Objector>::From(entity).CreateAndSetup());
        _already_new_tuple = false;
    } catch (boost::python::error_already_set&) {
        PyErr_Print();
        CHECK(false);
    }
}

void TransformProcessor::begin_group(const std::vector<toft::StringPiece>& keys,
                                     ProcessorContext* context) {
    _context = context;
    _emitter_delegator.set_emitter(context->emitter());
    _emitter = boost::python::object(boost::ref(_emitter_delegator));
    _done_fn.reset(toft::NewPermanentClosure(context, &ProcessorContext::done));
    int size = context->python_side_inputs().size();

    if (!_already_new_tuple) {
        _init_args = new_tuple(1 + size);
        _args = new_tuple(3 + size);
        _final_args = new_tuple(2 + size);
        _already_new_tuple = true;
    }
    fill_args(1, context->python_side_inputs(), &_init_args);
    tuple_set_item(_init_args, 0, _emitter);

    fill_args(3, context->python_side_inputs(), &_args);
    tuple_set_item(_args, 0, (*_initializer_fn)(_done_fn.get(), &_init_args));
    tuple_set_item(_args, 1, _emitter);
}

void TransformProcessor::process(void* object) {
    boost::python::object input = *static_cast<boost::python::object*>(object);
    tuple_set_item(_args, 2, input);
    tuple_set_item(_args, 0, (*_transformer_fn)(_done_fn.get(), &_args));
}

void TransformProcessor::end_group() {
    fill_args(2, _context->python_side_inputs(), &_final_args);
    tuple_set_item(_final_args, 0, tuple_get_item(_args, 0));
    tuple_set_item(_final_args, 1, _emitter);
    (*_finalizer_fn)(_done_fn.get(), &_final_args);
}

uint32_t TransformProcessor::serialize(char* buffer, uint32_t buffer_size) {
    boost::python::object status = tuple_get_item(_args, 0);
    return _objector->Serialize(&status, buffer, buffer_size);
}

bool TransformProcessor::deserialize(const char* buffer, uint32_t buffer_size) {
    try {
        boost::python::object* deserialized =
            (boost::python::object *)_objector->Deserialize(buffer, buffer_size);
        tuple_set_item(_args, 0, *deserialized);
        _objector->Release(deserialized);
        return true;
    } catch (boost::python::error_already_set&) {
        PyErr_Print();
        CHECK(false);
    }
}

} // namespace python
} // namespace bigflow
} // namespace baidu
