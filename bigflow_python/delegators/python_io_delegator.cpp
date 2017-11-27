/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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

/*
* @Author: zhangyuncong
* @Date:   2016-01-08 13:42:54
* @Last Modified by:   zhangyuncong
* @Last Modified time: 2016-06-24 16:02:38
*/

#include "boost/python.hpp"

#include "python_io_delegator.h"

#include <exception>

#include "boost/ptr_container/ptr_vector.hpp"
#include "glog/logging.h"
#include "toft/base/string/string_piece.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/serde/cpickle_serde.h"

#include "flume/proto/entity.pb.h"
#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/core/partitioner.h"

namespace baidu {
namespace bigflow {
namespace python {

namespace {

class PythonLoaderImpl : public flume::core::Loader{
public:
    virtual void Setup(const std::string& config) {
        try {
            _loader = _cpickle.loads(config);
        } catch (boost::python::error_already_set&) {
            BIGFLOW_HANDLE_PYTHON_EXCEPTION();
        }
    }

    virtual void Split(const std::string& uri, std::vector<std::string>* splits) {
        try {
            boost::python::object ret = _loader.attr("split")(uri);
            PyObject* iter = PyObject_GetIter(ret.ptr());
            CHECK(iter != NULL) << "call iter method failed, split must return an iterable object";

            PyObject* item = NULL;
            while ((item = PyIter_Next(iter)) != NULL) {
                boost::python::object obj =
                    boost::python::object(boost::python::handle<>(item));
                obj = _cpickle.dumps(obj);
                splits->push_back(str_get_buf(obj).as_string());
            }
            Py_DECREF(iter);
        } catch (boost::python::error_already_set&) {
            BIGFLOW_HANDLE_PYTHON_EXCEPTION();
        }
    }

    virtual void Load(const std::string& split, flume::core::Emitter* emitter) {
        try {
            boost::python::object split_obj = _cpickle.loads(split);
            boost::python::object ret = _loader.attr("load")(split_obj);
            PyObject* iter = PyObject_GetIter(ret.ptr());
            CHECK(iter != NULL) << "call iter() method failed, load must return an iterable object";

            PyObject* item = NULL;
            while ((item = PyIter_Next(iter)) != NULL) {
                boost::python::object obj =
                    boost::python::object(boost::python::handle<>(item));
                emitter->Emit(&obj);
            }
            Py_DECREF(iter);
        } catch (boost::python::error_already_set&) {
            BIGFLOW_HANDLE_PYTHON_EXCEPTION();
        }
    }

private:
    boost::python::object _loader;
    CPickleSerde _cpickle;
};

class PythonSinkerImpl : public flume::core::Sinker{
public:
    virtual void Setup(const std::string& config) {
        try {
            boost::python::object sinker = CPickleSerde().loads(config);
            _open = sinker.attr("open");
            _close = sinker.attr("close");
            _sink = sinker.attr("sink");
            boost::python::list key_serde_entities = boost::python::list(sinker.attr("key_serdes"));
            size_t len = boost::python::len(key_serde_entities);
            flume::PbEntity entity;
            for (size_t i = 0; i != len; ++i) {
                toft::StringPiece entity_pb_string = str_get_buf(key_serde_entities[i]);
                entity.ParseFromArray(entity_pb_string.data(), entity_pb_string.size());
                _key_serdes.push_back(
                    flume::core::Entity<flume::core::Objector>::From(entity).CreateAndSetup());
            }
        } catch (boost::python::error_already_set&) {
            BIGFLOW_HANDLE_PYTHON_EXCEPTION();
        }
    }

    virtual void Open(const std::vector<toft::StringPiece>& keys) {
        try {
            CHECK_EQ(keys.size(), _key_serdes.size() + 1);
            boost::python::list key_list;
            for (size_t i = 1; i != keys.size(); ++i) {
                void* key = _key_serdes[i - 1].Deserialize(keys[i].data(), keys[i].size());
                boost::python::object* key_obj = static_cast<boost::python::object*>(key);
                list_append_item(key_list, *key_obj);
                _key_serdes[i - 1].Release(key);
            }
            _open(key_list);
        } catch (boost::python::error_already_set&) {
            BIGFLOW_HANDLE_PYTHON_EXCEPTION();
        }
    }

    virtual void Sink(void* object) {
        try {
            _sink(*static_cast<boost::python::object*>(object));
        } catch (boost::python::error_already_set&) {
            BIGFLOW_HANDLE_PYTHON_EXCEPTION();
        }
    }

    virtual void Close() {
        try {
            _close();
        } catch (boost::python::error_already_set&) {
            BIGFLOW_HANDLE_PYTHON_EXCEPTION();
        }
    }

 private:
    boost::python::object _open;
    boost::python::object _sink;
    boost::python::object _close;
    boost::ptr_vector<flume::core::Objector> _key_serdes;
 };

} // namespace

PythonLoaderDelegator::PythonLoaderDelegator() {
    _impl.reset(new PythonLoaderImpl);
}

void PythonLoaderDelegator::Setup(const std::string& config) {
    _impl->Setup(config);
}

void PythonLoaderDelegator::Split(const std::string& uri, std::vector<std::string>* splits) {
    _impl->Split(uri, splits);
}

void PythonLoaderDelegator::Load(const std::string& split, flume::core::Emitter* emitter) {
    _impl->Load(split, emitter);
}

PythonSinkerDelegator::PythonSinkerDelegator() {
    _impl.reset(new PythonSinkerImpl);
}

void PythonSinkerDelegator::Setup(const std::string& config) {
    _impl->Setup(config);
}

void PythonSinkerDelegator::Open(const std::vector<toft::StringPiece>& keys) {
    _impl->Open(keys);
}

void PythonSinkerDelegator::Sink(void* object) {
    _impl->Sink(object);
}

void PythonSinkerDelegator::Close() {
    _impl->Close();
}

void EncodePartitionObjector::Setup(const std::string& config) {
}

uint32_t EncodePartitionObjector::Serialize(void* object, char* buffer, uint32_t buffer_size) {
    uint32_t partition =
        boost::python::extract<uint32_t>(*static_cast<boost::python::object*>(object));
    std::string encoded = flume::core::EncodePartition(partition);
    if (buffer_size >= encoded.size()) {
        encoded.copy(buffer, encoded.size());
    }
    return encoded.size();
}

void* EncodePartitionObjector::Deserialize(const char* buffer, uint32_t buffer_size) {
    uint32_t partition = flume::core::DecodePartition(toft::StringPiece(buffer, buffer_size));
    return new boost::python::object(partition);
}

void EncodePartitionObjector::Release(void* object) {
    delete static_cast<boost::python::object*>(object);
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu
