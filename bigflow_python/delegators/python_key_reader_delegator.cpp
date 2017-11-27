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
// A delegating class for python KeyReaders.

#include "python2.7/Python.h"

#include "python_key_reader_delegator.h"

#include "glog/logging.h"

#include "flume/core/entity.h"
#include "flume/core/objector.h"
#include "flume/proto/entity.pb.h"

#include "bigflow_python/common/comparable.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/serde/cpickle_serde.h"

namespace baidu {
namespace bigflow {
namespace python {

class PythonKeyReaderDelegator::ReadKeyFn {
public:
    ReadKeyFn(const boost::python::object& read_key_fn)
            : _use_default_read_key_fn(false),
            _read_key_fn(read_key_fn) {
        if (read_key_fn == boost::python::object()) {
            _use_default_read_key_fn = true;
        }
    }

    boost::python::object user_define_read_key(const boost::python::object& obj) {
        return _read_key_fn(obj);
    }

    boost::python::object default_read_key(const boost::python::object& obj) {
        if (is_tuple(obj)) {
            return tuple_get_item(obj, 0);
        }
        if (is_list(obj)) {
            return list_get_item(obj, 0);
        }
        return obj[0];
    }

    boost::python::object operator()(const boost::python::object& obj) {
        if (_use_default_read_key_fn) {
            return default_read_key(obj);
        }
        return user_define_read_key(obj);
    }

private:
    bool _use_default_read_key_fn;
    boost::python::object _read_key_fn;
};

void PythonKeyReaderDelegator::Setup(const std::string& config) {
    using flume::core::Entity;
    using flume::core::Objector;
    try {
        boost::python::object key_reader = CPickleSerde().loads(config);
        _read_key_fn.reset(new ReadKeyFn(key_reader.attr("read_key")));
        std::string objector_str = boost::python::extract<std::string>(key_reader.attr("objector"));
        baidu::flume::PbEntity entity;
        CHECK(entity.ParseFromArray(objector_str.data(), objector_str.size()));
        _objector.reset(Entity<Objector>::From(entity).CreateAndSetup());

    } catch (boost::python::error_already_set&) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

uint32_t PythonKeyReaderDelegator::ReadKey(void* object, char* buffer, uint32_t buffer_size) {
    try {
        boost::python::object key = (*_read_key_fn)(*static_cast<boost::python::object*>(object));
        return _objector->Serialize(&key, buffer, buffer_size);
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
    return 0;
}

PythonKeyReaderDelegator::PythonKeyReaderDelegator() {}

PythonKeyReaderDelegator::~PythonKeyReaderDelegator() {}

StrKeyReaderDelegator::StrKeyReaderDelegator() {}

StrKeyReaderDelegator::~StrKeyReaderDelegator() {}

class StrKeyReaderDelegator::ReadKeyFn {
private:
    bool is_multiple_element(const boost::python::object& object) {
        return is_list(object) || is_tuple(object);
    }
    boost::python::object get_element_by_index(const boost::python::object& object, int64_t idx) {
        if (is_list(object)) {
            return list_get_item(object, idx);
        } else if (is_tuple(object)) {
            return tuple_get_item(object, idx);
        } else {
            CHECK(false) << "Not a valid multiple elements type";
        }
    }
    uint32_t serialize_multiple_key(const boost::python::object& pyobj, \
        char* buffer, uint32_t buffer_size) {
        int64_t element_len = object_len(pyobj);
        uint32_t result = 0;
        for (int64_t i = 0; i < element_len; ++i) {
            boost::python::object key_obj = get_element_by_index(pyobj, i);
            result += serialize_single_key(key_obj, buffer + result, \
                SafeDec(buffer_size, result));
        }
        return result;
    }

    uint32_t serialize_python_long_type(const boost::python::object& key, \
        char *buffer, uint32_t buffer_size, bool reverse) {
        int32_t sign = Py_SIZE(key.ptr());

        uint32_t result = 0;
        if (!reverse) {
            result += AppendOrdered(sign, buffer + result, \
                SafeDec(buffer_size, result));
        } else {
            result += AppendReverseOrdered(sign, buffer + result, \
                SafeDec(buffer_size, result));
        }
        result += python_long_as_buffer((PyLongObject*)key.ptr(), \
            buffer + result, SafeDec(buffer_size, result), reverse);
        return result;

    }
    uint32_t serialize_single_key(const boost::python::object& obj, \
        char *buffer, uint32_t buffer_size){
        bool reverse = false;
        boost::python::object key;
        if (PyObject_HasAttrString(obj.ptr(), "key")) {
            if (PyObject_HasAttrString(obj.ptr(), "reverse")) {
                reverse = obj.attr("reverse");
            } else {
                reverse = _reverse;
            }
            key = obj.attr("key");
        } else {
            key = obj;
            reverse = _reverse;
        }

        if (is_int(key)) {
            int64_t key_int = boost::python::extract<int64_t>(key);
            if (reverse) {
                return AppendReverseOrdered(key_int, buffer, buffer_size);
            }
            return AppendOrdered(key_int, buffer, buffer_size);
        } else if (is_python_long(key)) {
            return serialize_python_long_type(key, buffer, buffer_size, reverse);
        } else if (is_python_string(key)) {
            std::string key_str = boost::python::extract<std::string>(key);
            if (reverse) {
                return AppendReverseOrdered<const std::string&>(key_str, buffer, buffer_size);
            }
            return AppendOrdered<const std::string&>(key_str, buffer, buffer_size);
        } else if (is_python_float(key)) {
            double d = ((PyFloatObject*)key.ptr())->ob_fval;
            if (reverse) {
                return AppendReverseOrdered<double>(d, buffer, buffer_size);
            }
            return AppendOrdered<double>(d, buffer, buffer_size);
        } else if (is_python_bool(key)) {
            bool b = boost::python::extract<bool>(key);
            if (reverse) {
                return AppendReverseOrdered<bool>(b, buffer, buffer_size);
            }
            return AppendOrdered<bool>(b, buffer, buffer_size);
        } else if (is_python_unicode(key)) {
            PyObject *utf8 = PyUnicode_AsUTF8String(key.ptr());
            char* utf8_as_string = NULL;
            Py_ssize_t str_len = 0;
            PyString_AsStringAndSize(utf8, &utf8_as_string, &str_len);
            if (str_len < 0) {
                CHECK(false) << "Get len of string from utf8 pyobject should not negative";
            }
            if (reverse) {
                return AppendReverseOrdered((const char*)utf8_as_string, \
                    static_cast<size_t>(str_len), buffer, buffer_size);
            }
            return AppendOrdered((const char*)utf8_as_string, \
                static_cast<size_t>(str_len), buffer, buffer_size);
        }
        return 0;
    }
public:
    ReadKeyFn(const boost::python::object& read_key_fn,
              bool reverse)
            : _use_default_read_key_fn(false),
            _read_key_fn(read_key_fn),
            _reverse(reverse) {
        if (read_key_fn == boost::python::object()) {
            _use_default_read_key_fn = true;
        }
    }

    uint32_t user_define_read_key(const boost::python::object& obj, \
        char *buffer, uint32_t buffer_size) {
            boost::python::object result = _read_key_fn(obj);
            if (is_multiple_element(result)) {
                return serialize_multiple_key(result, buffer, buffer_size);
            }
            return serialize_single_key(result, buffer, buffer_size);
    }

    uint32_t default_read_key(const boost::python::object& obj, \
        char *buffer, uint32_t buffer_size) {
        if (is_multiple_element(obj)) {
            return serialize_multiple_key(obj, buffer, buffer_size);
        }
        return serialize_single_key(obj, buffer, buffer_size);
    }

    uint32_t operator()(const boost::python::object& obj, char *buffer, uint32_t buffer_size) {
        if (_use_default_read_key_fn) {
            return default_read_key(obj, buffer, buffer_size);
        }
        return user_define_read_key(obj, buffer, buffer_size);
    }

private:
    bool _use_default_read_key_fn;
    boost::python::object _read_key_fn;
    bool _reverse;
};

void StrKeyReaderDelegator::Setup(const std::string& config) {
    boost::python::object key_reader = CPickleSerde().loads(config);
    _read_key_fn.reset(new ReadKeyFn(key_reader.attr("read_key"), key_reader.attr("reverse")));
}

uint32_t StrKeyReaderDelegator::ReadKey(void* object, char* buffer, uint32_t buffer_size) {
    try {
        return (*_read_key_fn)(*static_cast<boost::python::object*>(object), buffer, buffer_size);
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

