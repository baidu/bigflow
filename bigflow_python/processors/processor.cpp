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
// Author: Zhang Yuncong <zhangyuncong@baidu.com>
//
// A high level delegating base class for Processors with side inputs.

#include "bigflow_python/processors/processor.h"

#include "boost/ptr_container/ptr_vector.hpp"

namespace baidu {
namespace bigflow {
namespace python {

class ProcessorContext::Impl {
public:
    Impl() : _side_inputs_type(NULL) {}

    void reset(flume::core::Emitter* emitter,
               std::vector<flume::core::Iterator*>* side_inputs,
               std::vector<PbDatasetType>* side_inputs_type = NULL) {
        _side_inputs_holder.clear();
        _python_side_inputs.clear();
        _already_get_side_inputs = false;
        _emitter = emitter;
        _side_inputs = side_inputs;
        _side_inputs_type = side_inputs_type;
        if (side_inputs_type != NULL) {
            CHECK_EQ(_side_inputs_type->size(), _side_inputs->size());
        }
    }

    flume::core::Emitter* emitter() const {
        return _emitter;
    }

    const std::vector<flume::core::Iterator*>& side_inputs() const {
        return *_side_inputs;
    }

    const std::vector<boost::python::object>& python_side_inputs() {
        if (_already_get_side_inputs) {
            return _python_side_inputs;
        }
        _already_get_side_inputs = true;
        for (size_t i = 0; i < _side_inputs->size(); i++) {
            CHECK_NOTNULL((*_side_inputs)[i]);
            if (_side_inputs_type == NULL || (*_side_inputs_type)[i] == PCOLLECTION_TYPE) {
                _side_inputs_holder.push_back(new SideInput((*_side_inputs)[i]));
                _python_side_inputs.push_back(boost::python::object(boost::ref(_side_inputs_holder.back())));
            } else if ((*_side_inputs_type)[i] == POBJECT_TYPE) {
                BIGFLOW_PYTHON_RAISE_EXCEPTION_IF(!(*_side_inputs)[i]->HasNext(),
                    "A PObject passed as SideInput is empty, which is not allowed.\n"
                     "You can call pobject.as_pcollection() to convert a PObject to a PCollection,\n"
                     "then pass the PCollection as SideInput.\n"
                     "(Of course you need to check whether the PCollection is empty or not in your transform)");
                boost::python::object object = *static_cast<boost::python::object*>(
                    (*_side_inputs)[i]->NextValue()
                );
                _python_side_inputs.push_back(object);
                (*_side_inputs)[i]->Done();
            } else {
                CHECK(false) << "unrecognized side inputs type:"
                             << static_cast<int>((*_side_inputs_type)[i]);
            }
        }
        return _python_side_inputs;
    }

private:
    flume::core::Emitter* _emitter;
    std::vector<flume::core::Iterator*>* _side_inputs;
    std::vector<PbDatasetType>* _side_inputs_type;

    boost::ptr_vector<SideInput> _side_inputs_holder;
    bool _already_get_side_inputs;
    std::vector<boost::python::object> _python_side_inputs;
};

void ProcessorContext::reset(flume::core::Emitter* emitter,
                             std::vector<flume::core::Iterator*>* side_inputs,
                             std::vector<PbDatasetType>* side_inputs_type) {
    _impl->reset(emitter, side_inputs, side_inputs_type);
}

ProcessorContext::ProcessorContext() : _impl(new ProcessorContext::Impl){}

ProcessorContext::ProcessorContext(flume::core::Emitter* emitter,
                                   std::vector<flume::core::Iterator*>& side_inputs,
                                   std::vector<PbDatasetType>& side_inputs_type)
                                   : _impl(new ProcessorContext::Impl) {
    _impl->reset(emitter, &side_inputs, &side_inputs_type);
}

ProcessorContext::ProcessorContext(flume::core::Emitter* emitter,
                                  std::vector<flume::core::Iterator*>& side_inputs)
                                  : _impl(new ProcessorContext::Impl) {
    _impl->reset(emitter, &side_inputs, NULL);
}

flume::core::Emitter* ProcessorContext::emitter() {
    return _impl->emitter();
}

bool ProcessorContext::emit(void* object) {
    return emitter()->Emit(object);
}

void ProcessorContext::done() {
    return emitter()->Done();
}

const std::vector<boost::python::object>& ProcessorContext::python_side_inputs() {
    return _impl->python_side_inputs();
}

const std::vector<flume::core::Iterator*>& ProcessorContext::side_inputs() {
    return _impl->side_inputs();
}

ProcessorContext::~ProcessorContext() {}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu
