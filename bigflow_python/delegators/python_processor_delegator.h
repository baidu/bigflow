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
// Author: Wang Cong <bigflow-opensource@baidu.com>
//
// A delegating class for python Processors.

#ifndef BIGFLOW_PYTHON_PYTHON_PROCESSOR_DELEGATOR_H_
#define BIGFLOW_PYTHON_PYTHON_PROCESSOR_DELEGATOR_H_

#include "boost/python.hpp"
#include <exception>

#include "glog/logging.h"
#include "toft/base/string/string_piece.h"
#include "toft/base/scoped_ptr.h"

#include "flume/core/processor.h"
#include "flume/core/emitter.h"

namespace baidu{
namespace flume{
namespace runtime{

class Record;

}  // runtime
}  // flume
}  // baidu

namespace baidu {
namespace bigflow {
namespace python {

// FlumeIteratorDelegator is used to wrap flume::core::Iterator*. It act like a
// Python Iterator by:
//
//  1. Exposing next() method
//  2. Throwing an StopIteration when iteration is done.
//
class FlumeIteratorDelegator {
public:
    // Will be translated to Python's StopIteration exception.
    struct StopIteration : std::exception {};

public:
    explicit FlumeIteratorDelegator(flume::core::Iterator* iter);
    // Copyable to put in std::vector
    FlumeIteratorDelegator(const FlumeIteratorDelegator& other);
    virtual ~FlumeIteratorDelegator();

    bool operator==(const FlumeIteratorDelegator& other) const;
    bool operator!=(const FlumeIteratorDelegator& other) const;

    virtual bool HasNext() const;
    virtual boost::python::object NextValue();
    virtual void Reset();
    virtual void Done();

private:
    flume::core::Iterator* _iter;
};

// // Used to wrap flume::core::Emitter*.
// class EmitterDelegator {
// public:
//     EmitterDelegator() : _emitter(NULL) {}

//     EmitterDelegator(flume::core::Emitter* emitter) : _emitter(emitter) {}

//     virtual ~EmitterDelegator() {}

//     void set_emitter(flume::core::Emitter* emitter) {
//         _emitter = emitter;
//     }

//     bool emit(PyObject* object) {
//         return _emitter->Emit(object);
//     }

//     void done() {
//         _emitter->Done();
//     }

// private:
//     flume::core::Emitter* _emitter;
// };

// Temporarily!
// we need to emit boost::python::object here
class EmitterDelegator {
public:
    EmitterDelegator() : _emitter(NULL) {}
    EmitterDelegator(flume::core::Emitter* emitter) : _emitter(emitter) {}
    virtual ~EmitterDelegator() {}

    void set_emitter(flume::core::Emitter* emitter) {
        _emitter = emitter;
    }

    bool emit(boost::python::object object) {
        return _emitter->Emit(&object);
    }

    void done() {
        _emitter->Done();
    }

private:
    flume::core::Emitter* _emitter;
};

class PythonProcessorDelegator : public flume::core::Processor {
public:
    PythonProcessorDelegator() {}
    virtual ~PythonProcessorDelegator() {}

    virtual void Setup(const std::string& config);

    virtual void BeginGroup(const std::vector<toft::StringPiece>& keys,
                            const std::vector<flume::core::Iterator*>& inputs,
                            flume::core::Emitter* emitter);

    virtual void Process(uint32_t index, void* object);

    virtual void EndGroup();

protected:
    boost::python::object _processor;
    EmitterDelegator _emitter;

    boost::python::object _begin;
    boost::python::object _process;
    boost::python::object _end;
};

class PythonIOProcessorBase : public flume::core::Processor {
public:
    virtual ~PythonIOProcessorBase() {}

    virtual void Setup(const std::string& config) {}

    virtual void BeginGroup(const std::vector<toft::StringPiece>& keys,
                            const std::vector<flume::core::Iterator*>& inputs,
                            flume::core::Emitter* emitter) {
        _emitter = emitter;
    }

    virtual void Process(uint32_t index, void* object) = 0;

    virtual void EndGroup() {}

protected:
    flume::core::Emitter* _emitter;
};

class PythonFromRecordProcessor : public PythonIOProcessorBase {
public:
    PythonFromRecordProcessor() {}
    virtual ~PythonFromRecordProcessor() {}

    virtual void Process(uint32_t index, void* object);
};

class PythonFromBigpipeRecordProcessor : public PythonIOProcessorBase {
public:
    PythonFromBigpipeRecordProcessor() {}
    virtual ~PythonFromBigpipeRecordProcessor() {}

    virtual void Process(uint32_t index, void* object);
};

class PythonKVFromRecordProcessor : public PythonIOProcessorBase {
public:
    PythonKVFromRecordProcessor() {
        _kv_tuple = PyTuple_New(2);
    }
    virtual ~PythonKVFromRecordProcessor() {
        Py_DECREF(_kv_tuple);
    }

    virtual void Process(uint32_t index, void* object);

protected:
    PyObject* _kv_tuple;
};

class PythonToBigpipeRecordProcessor : public PythonIOProcessorBase {
public:
    PythonToBigpipeRecordProcessor() {}
    virtual ~PythonToBigpipeRecordProcessor() {}

    virtual void Process(uint32_t index, void* object);

protected:
    std::string _record;
};

class PythonToRecordProcessor : public PythonIOProcessorBase {
public:
    PythonToRecordProcessor();
    virtual ~PythonToRecordProcessor();

    virtual void Process(uint32_t index, void* object);
    //virtual flume::runtime::Record* mutable_record() = 0;
protected:
    class Impl;
    toft::scoped_ptr<Impl> _impl;
};

class PythonKVToRecordProcessor : public PythonToRecordProcessor {
public:
    PythonKVToRecordProcessor() {}
    virtual ~PythonKVToRecordProcessor() {}

    virtual void Process(uint32_t index, void* object);
    virtual flume::runtime::Record* mutable_record();
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

#endif  // BIGFLOW_PYTHON_PYTHON_PROCESSOR_DELEGATOR_H_
