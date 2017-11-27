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
// Author: Zhang Yuncong <bigflow-opensource@baidu.com>
//
// A high level delegating base class for Processors with side inputs.

#ifndef BIGFLOW_PYTHON_PROCESSOR_H_
#define BIGFLOW_PYTHON_PROCESSOR_H_

#include "boost/python.hpp"

#include <exception>

#include "boost/ptr_container/ptr_vector.hpp"
#include "glog/logging.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/delegators/python_processor_delegator.h"
#include "bigflow_python/functors/functor.h"
#include "bigflow_python/proto/processor.pb.h"
#include "bigflow_python/python_interpreter.h"

#include "flume/core/processor.h"
#include "flume/core/emitter.h"
#include "flume/core/entity.h"

#include "flume/runtime/counter.h"

namespace baidu {
namespace bigflow {
namespace python {

class ProcessorContext;

// user should derived from this class
class Processor {
public:

    // Setup for processor
    // @param fns: the functors may be needed by this processor
    // @param config: other config for this processor
    virtual void setup(const std::vector<Functor*>& fns, const std::string& config) = 0;

    // Begin to process a group of data
    // @param keys: the keys of the incoming group
    // @param context: the context of this group.
    virtual void begin_group(const std::vector<toft::StringPiece>& keys,
                             ProcessorContext* context) = 0;

    // Process input data, this method will be called when the data come
    // @param object: input record
    virtual void process(void* object) = 0;

    // Called when a group of data is end
    virtual void end_group() = 0;

    // Serialize the processor
    // This method is used for stream processing,
    // Override this function to dump status info so that it can be resumed on failover.
    //
    // @param buffer: target buffer
    // @param buffer_size: the size of the target buffer
    // @return used buffer size if the input buffer is enough.
    //         If it's not enough, this method should return the needed size.
    //         And another call with enough size of buffer should be made.
    virtual uint32_t serialize(char* buffer, uint32_t buffer_size) { return 0; }

    // Deserialize the processor
    // This method is used for stream processing,
    // Override this function to deserialize the processor on failover.
    //
    // @param buffer: source buffer
    // @param buffer_size: the size of the source buffer
    // @return true of false, indicate if deserialize success
    virtual bool deserialize(const char* buffer, uint32_t buffer_size) { return true; }

    virtual ~Processor() {}
};

class ProcessorContext {
public:
    ProcessorContext();

    // Construct a Processor Context
    //
    // @param emitter: used for emitting data to downstream
    // @param side_inputs: the prepared inputs
    // @param side_inputs_type: type of side inputs, such as PObject, PCollection
    ProcessorContext(flume::core::Emitter* emitter,
                     std::vector<flume::core::Iterator*>& side_inputs,
                     std::vector<PbDatasetType>& side_inputs_type);

    ProcessorContext(flume::core::Emitter* emitter,
                     std::vector<flume::core::Iterator*>& side_inputs);

    void reset(flume::core::Emitter* emitter,
               std::vector<flume::core::Iterator*>* side_inputs,
               std::vector<PbDatasetType>* side_inputs_type = NULL);

    // get side inputs and deserialize them to python objects
    const std::vector<boost::python::object>& python_side_inputs();

    // get the serialized side inputs
    const std::vector<flume::core::Iterator*>& side_inputs();

    // get the emitter, user can use emitter()->Emit(object) to emit data to downstream
    flume::core::Emitter* emitter();

    // emit data to downstream, equal to emitter()->Emit(object)
    // @return: indicate whether the downstreams need data any more.
    //          If all the downstream called `done`,
    //          this method will return false.
    bool emit(void* object);

    // tells the upstream the process doesn't need data any more
    // and tells the downstream this processor will emit data no more.
    void done();

    ~ProcessorContext();

private:
    class Impl;
    toft::scoped_ptr<Impl> _impl;
};

template<typename ProcessorType, int NormalInputNum = 1>
class ProcessorImpl : public flume::core::Processor {
public:
    ProcessorImpl() {}

    virtual void Setup(const std::string& config);

    virtual void BeginGroup(const std::vector<toft::StringPiece>& keys,
                            const std::vector<flume::core::Iterator*>& inputs,
                            flume::core::Emitter* emitter);

    virtual void Process(uint32_t index, void* object);

    virtual void EndGroup();

    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size);

    virtual bool Deserialize(const char* buffer, uint32_t buffer_size);

    void UpdateCounters();

private:
    ProcessorType _processor;
    toft::scoped_ptr<std::vector<flume::core::Iterator*> > _side_inputs;
    bool _is_first_time;
    toft::scoped_ptr<ProcessorContext> _context;
    boost::ptr_vector<Functor> _functors;
    std::vector<PbDatasetType> _side_inputs_type;
};


template<typename ProcessorType, int NormalInputNum>
void ProcessorImpl<ProcessorType, NormalInputNum>::Setup(const std::string& config) {
    PbPythonProcessorConfig pb_config;
    try {
        CHECK(pb_config.ParseFromArray(config.data(), config.size()));
        std::vector<Functor*> fns;
        using flume::core::Entity;
        for (size_t i = 0; i != pb_config.functor_size(); ++i) {
            _functors.push_back(Entity<Functor>::From(pb_config.functor(i)).CreateAndSetup());
            fns.push_back(&_functors.back());
        }
        for (size_t i = 0; i != pb_config.side_input_type_size(); ++i) {
            _side_inputs_type.push_back(pb_config.side_input_type(i));
        }
        std::string sub_config;
        if (pb_config.has_config()) {
            sub_config = pb_config.config();
        }
        _processor.setup(fns, sub_config);
    } catch (boost::python::error_already_set&) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
    _is_first_time = true;
    _context.reset(new ProcessorContext);
}

template<typename ProcessorType, int NormalInputNum>
void ProcessorImpl<ProcessorType, NormalInputNum>::BeginGroup(
        const std::vector<toft::StringPiece>& keys,
        const std::vector<flume::core::Iterator*>& inputs,
        flume::core::Emitter* emitter) {
    try {
        CHECK_GE(inputs.size(), NormalInputNum);
        if (_is_first_time) {
            _side_inputs.reset(
                new std::vector<flume::core::Iterator*>(inputs.begin(), inputs.end() - NormalInputNum)
            );
            _is_first_time = false;
        } else {
            (*_side_inputs).resize(inputs.size() - NormalInputNum);
            std::copy(inputs.begin(), inputs.end() - NormalInputNum, (*_side_inputs).begin());
        }
        _context->reset(emitter, _side_inputs.get(), &_side_inputs_type);
        _processor.begin_group(keys, _context.get());
    } catch (boost::python::error_already_set&) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

template<typename ProcessorType, int NormalInputNum>
void ProcessorImpl<ProcessorType, NormalInputNum>::Process(uint32_t index, void* object) {
    try {
        _processor.process(object);
    } catch (boost::python::error_already_set&) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

template<typename ProcessorType, int NormalInputNum>
void ProcessorImpl<ProcessorType, NormalInputNum>::EndGroup() {
    try {
        _processor.end_group();
    } catch (boost::python::error_already_set&) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

template<typename ProcessorType, int NormalInputNum>
void ProcessorImpl<ProcessorType, NormalInputNum>::UpdateCounters() {
    try {
        boost::python::object counter = boost::python::import("bigflow.counter");
        boost::python::dict counters = boost::python::extract<boost::python::dict>(counter.attr("counter_dict"));

        boost::python::list keys = counters.keys();
        for (int i = 0; i < len(keys); ++i) {
            boost::python::extract<std::string> extracted_key(keys[i]);
            CHECK(extracted_key.check());
            std::string key = extracted_key;
            boost::python::extract<uint64_t> extracted_val(counters[key]);
            CHECK(extracted_val.check());
            uint64_t value = extracted_val;
            baidu::flume::runtime::CounterSession::GlobalCounterSession()->GetCounter(key)->Update(value);
            counters[key] = boost::python::object(0);
        }
        std::string processor_name = "CounterUpdates|" + flume::PrettyTypeName<ProcessorType>();
        baidu::flume::runtime::CounterSession::GlobalCounterSession()->GetCounter(processor_name)->Update(1);
    } catch (boost::python::error_already_set&) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

template<typename ProcessorType, int NormalInputNum>
uint32_t ProcessorImpl<ProcessorType, NormalInputNum>::Serialize(
        char* buffer,
        uint32_t buffer_size) {
    try {
        return _processor.serialize(buffer, buffer_size);
    } catch (boost::python::error_already_set&) {
        PyErr_Print();
        CHECK(false);
    }
}

template<typename ProcessorType, int NormalInputNum>
bool ProcessorImpl<ProcessorType, NormalInputNum>::Deserialize(
        const char* buffer,
        uint32_t buffer_size) {
    try {
        return _processor.deserialize(buffer, buffer_size);
    } catch (boost::python::error_already_set&) {
        PyErr_Print();
        CHECK(false);
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_PYTHON_PROCESSOR_H_
