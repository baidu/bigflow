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
// Author: Wang Cong <wangcong09@baidu.com>
//         Zhang Yuncong <zhangyuncong@baidu.com>
//
// A delegating class for python Processors.


#include "python_processor_delegator.h"

#include "glog/logging.h"
#include "boost/foreach.hpp"

#include "flume/runtime/io/io_format.h"
#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"
#include "bigflow_python/python_interpreter.h"

namespace baidu {
namespace bigflow {
namespace python {

// FlumeIteratorDelegator implmenetation
FlumeIteratorDelegator::FlumeIteratorDelegator(flume::core::Iterator* iter) : _iter(iter) {}

FlumeIteratorDelegator::FlumeIteratorDelegator(const FlumeIteratorDelegator& other) {
    this->_iter = other._iter;
}

FlumeIteratorDelegator::~FlumeIteratorDelegator() {}

bool FlumeIteratorDelegator::operator==(const FlumeIteratorDelegator& other) const {
    return other._iter == this->_iter;
}

bool FlumeIteratorDelegator::operator!=(const FlumeIteratorDelegator& other) const {
    return other._iter != this->_iter;
}

bool FlumeIteratorDelegator::HasNext() const {
    if (_iter == NULL) {
        return false;
    }
    return _iter->HasNext();
}

boost::python::object FlumeIteratorDelegator::NextValue() {
    if (this->HasNext()) {
        void* obj = _iter->NextValue();
        return *static_cast<boost::python::object*>(obj);
    } else {
        throw StopIteration();
    }
}

void FlumeIteratorDelegator::Reset() {
    _iter->Reset();
}

void FlumeIteratorDelegator::Done() {
    _iter->Done();
}

// PythonProcessorDelegator implmenetation
void PythonProcessorDelegator::Setup(const std::string& config) {
    _processor = CPickleSerde().loads(config);

    _begin = _processor.attr("begin");
    _process = _processor.attr("process");
    _end = _processor.attr("end");
}

void PythonProcessorDelegator::BeginGroup(const std::vector<toft::StringPiece>& keys,
                                          const std::vector<flume::core::Iterator*>& inputs,
                                          flume::core::Emitter* emitter) {
    std::vector<FlumeIteratorDelegator> _inputs;
    BOOST_FOREACH(flume::core::Iterator* iter, inputs) {
        _inputs.push_back(FlumeIteratorDelegator(iter));
    }

    try {
        _emitter.set_emitter(emitter);
        _begin(boost::ref(keys), boost::ref(_inputs), boost::ref(_emitter));
    } catch ( boost::python::error_already_set ) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

void PythonProcessorDelegator::Process(uint32_t index, void* object) {

    try {
        _process(index, *static_cast<boost::python::object*>(object));
    } catch ( boost::python::error_already_set ) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

void PythonProcessorDelegator::EndGroup() {
    try {
        _end();
    } catch ( boost::python::error_already_set ) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

void PythonFromRecordProcessor::Process(uint32_t index, void* object) {
    flume::runtime::Record* record = static_cast<flume::runtime::Record*>(object);

    try {
        boost::python::str str(record->value.data(), record->value.size());
        boost::python::object ret(str);
        if (!_emitter->Emit(&ret)) {
            _emitter->Done();
        }
    } catch ( boost::python::error_already_set ) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

void PythonFromBigpipeRecordProcessor::Process(uint32_t index, void* object) {
    std::string* record = static_cast<std::string*>(object);

    try {
        boost::python::str str(record->data(), record->size());
        boost::python::object ret(str);
        if (!_emitter->Emit(&ret)) {
            _emitter->Done();
        }
    } catch (boost::python::error_already_set) {
        PyErr_Print();
        CHECK(false);
    }
}

void PythonKVFromRecordProcessor::Process(uint32_t index, void* object) {
    flume::runtime::Record* record = static_cast<flume::runtime::Record*>(object);

    try {
        boost::python::str key(record->key.data(), record->key.size());
        boost::python::str value(record->value.data(), record->value.size());

        boost::python::object tmp(boost::python::make_tuple(key, value));
        if (!_emitter->Emit(&tmp)) {
            _emitter->Done();
        }
    } catch ( boost::python::error_already_set ) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

class PythonToRecordProcessor::Impl {
public:
    void Process(uint32_t index, void* object, flume::core::Emitter* emitter) {
        char* str;
        Py_ssize_t len;
        try {
            boost::python::str obj_str(*static_cast<boost::python::object*>(object));
            PyString_AsStringAndSize(obj_str.ptr(), &str, &len);
            _record.value = toft::StringPiece(str, len);
            if (!emitter->Emit(&_record)) {
                emitter->Done();
            }
        } catch ( boost::python::error_already_set ) {
            BIGFLOW_HANDLE_PYTHON_EXCEPTION();
        }
    }

    flume::runtime::Record* mutable_record() {
        return &_record;
    }
private:
    flume::runtime::Record _record;
};

PythonToRecordProcessor::PythonToRecordProcessor() {
    _impl.reset(new Impl);
}

PythonToRecordProcessor::~PythonToRecordProcessor() {
}

void PythonToRecordProcessor::Process(uint32_t index, void* object) {
    _impl->Process(index, object, _emitter);
}

// flume::runtime::Record* PythonToRecordProcessor::mutable_record() {
//     return _impl->mutable_record();
// }

flume::runtime::Record* PythonKVToRecordProcessor::mutable_record() {
    return _impl->mutable_record();
}

void PythonToBigpipeRecordProcessor::Process(uint32_t index, void* object) {
    char* str = NULL;
    Py_ssize_t len = 0;
    try {
        boost::python::str obj_str(*static_cast<boost::python::object*>(object));
        PyString_AsStringAndSize(obj_str.ptr(), &str, &len);
        _record = std::string(str, len);
        if (!_emitter->Emit(&_record)) {
            _emitter->Done();
        }
    } catch (boost::python::error_already_set) {
        PyErr_Print();
        CHECK(false);
    }
}

void PythonKVToRecordProcessor::Process(uint32_t index, void* object) {

    try {
        Py_ssize_t key_size;
        char* key;
        Py_ssize_t value_size;
        char* value;

        PyObject* obj = static_cast<boost::python::object*>(object)->ptr();

        PyObject* key_str = PyTuple_GetItem(obj, 0);
        PyObject* value_str = PyTuple_GetItem(obj, 1);

        // TypeError will be raised on failure, do not need to check return value
        PyString_AsStringAndSize(key_str, &key, &key_size);
        PyString_AsStringAndSize(value_str, &value, &value_size);

        mutable_record()->key = toft::StringPiece(key, key_size);
        mutable_record()->value = toft::StringPiece(value, value_size);

        if (!_emitter->Emit(mutable_record())) {
            _emitter->Done();
        }
    } catch ( boost::python::error_already_set ) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

boost::python::object SideInput::get_iterator() {
    try {
        return as_list().attr("__iter__")();
    } catch ( boost::python::error_already_set ) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

boost::python::object SideInput::length() {
    std::vector<boost::python::object> side_input_vec = as_vector();
    return boost::python::object(side_input_vec.size());
}

boost::python::object SideInput::get_item(int n) {
    std::vector<boost::python::object> side_input_vec = as_vector();
    CHECK_LT(n, side_input_vec.size());
    return side_input_vec[n];
}

boost::python::object SideInput::as_list() {
    try {
        if (_already_get_list_iterator) {
            return _side_input_list;
        }
        if (!_already_get_vec_iterator) {
            as_vector();
        }
        _already_get_list_iterator = true;
        for (unsigned int i = 0; i < _side_input_vec.size(); i++) {
            _side_input_list.append(_side_input_vec[i]);
        }
        return _side_input_list;
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

const std::vector<boost::python::object>& SideInput::as_vector() {
    try {
        if (_already_get_vec_iterator) {
            return _side_input_vec;
        }
        _already_get_vec_iterator = true;
        _iterator->Reset();
        while (_iterator->HasNext()) {
            boost::python::object* pyobj = static_cast<boost::python::object*>(_iterator->NextValue());
            _side_input_vec.push_back(*pyobj);
        }
        _iterator->Done();
        return _side_input_vec;
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

