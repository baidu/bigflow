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

#include "bigflow_python/common/side_input.h"
#include "bigflow_python/common/python.h"
#include "glog/logging.h"
#include "boost/python/slice.hpp"
#include "gflags/gflags.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/storage/file/file.h"
#include "toft/base/scoped_ptr.h"
#include "longintrepr.h" // move from python.h to python.cpp as it leaks BASE macro.

#include "flume/util/path_util.h"

#include "bigflow_python/common/comparable.h"


namespace baidu {
namespace bigflow {
namespace python {

boost::python::object tuple_get_item(const boost::python::object& tuple, int n) { // NOLINT
    return boost::python::object((
        boost::python::handle<>(boost::python::borrowed(PyTuple_GET_ITEM(tuple.ptr(), n)))
    ));
}

void tuple_set_item(const boost::python::object& tuple,
                    int n,
                    const boost::python::object& object) {
    Py_INCREF(object.ptr());
    PyTuple_SetItem(tuple.ptr(), n, object.ptr());
}

boost::python::object list_get_item(const boost::python::object& list, int n) {
    return boost::python::object((
        boost::python::handle<>(boost::python::borrowed(PyList_GET_ITEM(list.ptr(), n)))
    ));
}

void list_set_item(const boost::python::object& list, int n, const boost::python::object& object) {
    Py_INCREF(object.ptr());
    PyList_SetItem(list.ptr(), n, object.ptr());
}

void list_append_item(const boost::python::object& list, const boost::python::object& object) {
    Py_INCREF(object.ptr());
    PyList_Append(list.ptr(), object.ptr());
}

boost::python::tuple new_tuple(int n) {
    return boost::python::tuple((boost::python::handle<>(PyTuple_New(n))));
}

int64_t list_len(const boost::python::object& list) {
    return PyList_GET_SIZE(list.ptr());
}

int64_t tuple_len(const boost::python::object& list) {
    return PyTuple_GET_SIZE(list.ptr());
}

//boost::python::tuple construct_args(int normal_arg, const boost::python::object& side_input) {
//    int size = list_len(side_input);
//    boost::python::tuple ret = new_tuple(size + normal_arg);
//    for (int i = 0; i != size; ++i) {
//        tuple_set_item(ret, normal_arg + i, list_get_item(side_input, i));
//    }
//    return ret;
//}

boost::python::tuple construct_args(int normal_arg,
                                    const std::vector<boost::python::object>& side_input) {
    int size = side_input.size();
    boost::python::tuple ret = new_tuple(size + normal_arg);
    for (int i = 0; i < size; i++) {
        tuple_set_item(ret, normal_arg + i, side_input[i]);
    }
    return ret;
}

void fill_args(int normal_arg, const std::vector<boost::python::object>& side_input,
               boost::python::tuple* ret) {
    int size = side_input.size();
    for (int i = 0; i < size; i++) {
        tuple_set_item(*ret, normal_arg + i, side_input[i]);
    }
}

boost::python::object side_input_list(const boost::python::object& side_input) {
    SideInput& ret = boost::python::extract<SideInput&>(side_input);
    return ret.as_list();
}

const std::vector<boost::python::object>& side_input_vec(const boost::python::object& side_input) {
    SideInput& ret = boost::python::extract<SideInput&>(side_input);
    return ret.as_vector();
}

bool is_list(const boost::python::object& object) {
    return PyList_CheckExact(object.ptr());
}

bool is_dict(const boost::python::object& object) {
    return PyDict_CheckExact(object.ptr());
}

bool is_tuple(const boost::python::object& object) {
    return PyTuple_CheckExact(object.ptr());
}

bool is_int(const boost::python::object& object) {
    return PyInt_CheckExact(object.ptr());
}

int64_t object_len(const boost::python::object& obj) {
    if (is_list(obj) || is_tuple(obj)) {
        return Py_SIZE(obj.ptr());
    }
    return boost::python::len(obj);
}

void dict_set_item(boost::python::object& dict,
        const boost::python::object& key,
        const boost::python::object& value) {
    PyDict_SetItem(dict.ptr(), key.ptr(), value.ptr());
}

toft::StringPiece str_get_buf(const boost::python::object& str) {
    char* s = NULL;
    Py_ssize_t len = 0;
    PyString_AsStringAndSize(str.ptr(), &s, &len);
    return toft::StringPiece(s, len);
}

boost::python::object tuple_get_slice(const boost::python::object& tp, int s, int e) {
    if (e < 0) {
        e += tuple_len(tp);
    }
    return boost::python::object(boost::python::handle<>(PyTuple_GetSlice(tp.ptr(), s, e)));
}

boost::python::object list_get_slice(const boost::python::object& obj, int s, int e) {
    if (e < 0) {
        e += list_len(obj);
    }
    return boost::python::object(boost::python::handle<>(PyList_GetSlice(obj.ptr(), s, e)));
}

boost::python::object object_get_slice(const boost::python::object& obj, int s, int e) {
    if (is_tuple(obj)) {
        return tuple_get_slice(obj, s, e);
    } else if (is_list(obj)) {
        return list_get_slice(obj, s, e);
    } else {
        return obj[boost::python::slice(s, e)];
    }
}

boost::python::object object_get_item(const boost::python::object& obj, int n) {
    if (is_tuple(obj)) {
        return tuple_get_item(obj, n);
    } else if (is_list(obj)) {
        return list_get_item(obj, n);
    } else {
        return obj[n];
    }
}

static uint32_t proc_digit(PyLongObject* v, int idx, int sign) {
    uint32_t digit = v->ob_digit[idx];
    uint32_t mask = (1 << PyLong_SHIFT) - 1;
    return digit & mask;
}

uint32_t python_long_as_buffer(PyLongObject* v, char *buffer, uint32_t buffer_size, bool reverse) {
    uint32_t digit_size = abs(Py_SIZE(v));
    int sign = Py_SIZE(v) > 0 ? 1 : -1;

    uint32_t pylong_mask = (1 << PyLong_SHIFT) - 1;

    uint32_t result = 0;
    if (sign < 0) {
        uint32_t carry = 0;
        std::vector<uint32_t> digits;

        uint32_t new_digit = ((pylong_mask ^ v->ob_digit[0]) + 1) & pylong_mask;
        carry = (((pylong_mask ^ v->ob_digit[0]) + 1) & (1ULL << PyLong_SHIFT)) >> PyLong_SHIFT;
        digits.push_back(new_digit);

        for (uint32_t i = 1; i < digit_size; ++i) {
            new_digit = ((pylong_mask ^ v->ob_digit[i]) + carry) & pylong_mask;
            carry = ((pylong_mask ^ v->ob_digit[i]) + carry) & (1ULL << PyLong_SHIFT);
            digits.push_back(new_digit);
        }

        for (int i = digits.size() - 1; i >= 0; --i) {
            if (!reverse) {
                AppendOrdered(digits[i] & pylong_mask, buffer + result, \
                    SafeDec(buffer_size, result));
            } else {
                AppendReverseOrdered(digits[i] & pylong_mask, buffer + result, \
                    SafeDec(buffer_size, result));
            }
            result += sizeof(digits[i] & pylong_mask);
        }
    } else {
        for (int32_t i = digit_size - 1; i >= 0; --i) {
            uint32_t digit = proc_digit(v, i, sign);
            if (!reverse) {
                AppendOrdered(digit, buffer + result, \
                    SafeDec(buffer_size, result));
            } else {
                AppendReverseOrdered(digit, buffer + result, \
                    SafeDec(buffer_size, result));
            }
            result += sizeof(digit);
        }
    }
    return result;
}

boost::python::object get_origin_serde(const boost::python::object& serde) {
    boost::python::object optional_obj = boost::python::import("bigflow.serde").attr("Optional");
    if (_PyObject_RealIsInstance(serde.ptr(), optional_obj.ptr())) {
        return get_origin_serde(serde.attr("origin_serde")());
    } else {
        return serde;
    }
}

std::string get_entity_reflect_str(const boost::python::object& entity) {
    boost::python::object entity_name = entity.attr("get_entity_name")();
    boost::python::object entity_config = entity.attr("get_entity_config")();
    std::string str_config = boost::python::extract<std::string>(entity_config);

    std::string name_key = boost::python::extract<std::string>(entity_name);
    boost::python::object entity_names = \
            ::boost::python::import("bigflow.core.entity_names");
    return boost::python::extract<std::string>(entity_names.attr("__dict__")[name_key]);
}

std::string get_entity_config_str(const boost::python::object& entity) {
    boost::python::object entity_config = entity.attr("get_entity_config")();
    return boost::python::extract<std::string>(entity_config);
}

namespace {

boost::python::object get_deep_copy_fn() {
    boost::python::object copy = boost::python::import("copy");
    return copy.attr("deepcopy");
}

} // namespace

boost::python::object deep_copy(const boost::python::object& obj) {
    static boost::python::object deepcopy = get_deep_copy_fn();
    return deepcopy(obj);
}

std::string format_exception() {
    boost::python::object traceback = boost::python::import("traceback");
    return boost::python::extract<std::string>(traceback.attr("format_exc")());
}

std::string type_string(const boost::python::object& obj) {
    return boost::python::extract<std::string>(obj.attr("__class__").attr("__name__"));
}

inline void atom_file_put_contents(const std::string& file_name, const std::string& content) {
    errno = 0;
    if (toft::File::Exists(file_name.c_str())) {
        return;
    } else {
        CHECK(errno == 0 || errno == ENOENT || errno == 115) << "unexpected errno = " << errno;
    }
    toft::scoped_ptr<toft::File> file;
    std::string tmp_file_name = file_name + '.' + toft::CreateCanonicalUUIDString();
    file.reset(toft::File::Open(tmp_file_name, "w"));
    CHECK_NOTNULL(file.get());
    CHECK_EQ(content.size(), file->Write(content.data(), content.size()));
    CHECK(file->Close());
    toft::File::Rename(tmp_file_name, file_name);
    CHECK(toft::File::Exists(file_name.c_str()));
}

boost::python::object get_formatted_exception() {
    try {
        PyObject *errtype = NULL;
        PyObject *errvalue = NULL;
        PyObject *trace = NULL;
        PyErr_Fetch(&errtype, &errvalue, &trace);
        CHECK_NOTNULL(errtype);
        // FIXME(wangcong09) according to CPython API, errvalue may be NULL
        CHECK_NOTNULL(errvalue);
        PyErr_NormalizeException(&errtype, &errvalue, &trace);

        boost::python::object type((boost::python::handle<>(errtype)));
        boost::python::object exception((boost::python::handle<>(errvalue)));
        boost::python::object mod_trace = boost::python::import("traceback");
        boost::python::object formated;

        if (trace != NULL) {
            boost::python::object traceback((boost::python::handle<>(trace)));
            formated = mod_trace.attr("format_exception")(type, exception, traceback);
        } else {
            // have no stacktrace
            formated = mod_trace.attr("format_exception_only")(type, exception);
        }

        formated = boost::python::str().attr("join")(formated);
        LOG(INFO) << std::string(boost::python::extract<std::string>(formated));

        boost::python::setattr(exception, "orig_args", exception.attr("args"));
        boost::python::setattr(exception, "orig_message", exception.attr("message"));
        boost::python::tuple args = boost::python::make_tuple(boost::python::str(formated));
        boost::python::setattr(exception, "args", args);
        return exception;
    } catch (boost::python::error_already_set) {
        PyErr_Print();
        CHECK(false);
    }
}

void write_exception_to_file(const std::string& file_name) {
    CHECK_NE(0, file_name.size());
    try {
        boost::python::object exception = get_formatted_exception();
        boost::python::object cloudpickle = boost::python::import("bigflow.core.serde.cloudpickle");
        boost::python::object dumped = cloudpickle.attr("dumps")(exception);
        std::string dumped_str = boost::python::extract<std::string>(dumped);
        atom_file_put_contents(file_name, dumped_str);
    } catch (boost::python::error_already_set) {
        PyErr_Print();
        CHECK(false);
    }
}

inline void write_exception_to_file(const std::string& file_name, const std::string& err_msg) {
    CHECK_NE(0, file_name.size());
    try {
        boost::python::object cloudpickle = boost::python::import("bigflow.core.serde.cloudpickle");
        boost::python::object dumped = cloudpickle.attr("dumps")(err_msg);
        std::string dumped_str = boost::python::extract<std::string>(dumped);
        atom_file_put_contents(file_name, dumped_str);
    } catch (boost::python::error_already_set) {
        PyErr_Print();
        CHECK(false);
    }
}

inline void write_serialized_exception_to_file(const std::string& file_name,
                                               const std::string& exception) {
    CHECK_NE(0, file_name.size());
    atom_file_put_contents(file_name, exception);
}


void throw_exception_to_client(const char* file, const char* function, int line_num) {
    std::string exception_path
        = google::StringFromEnv("BIGFLOW_PYTHON_EXCEPTION_TOFT_STYLE_PATH", "");
    write_exception_to_file(exception_path);
    LOG(FATAL) << "Exception occurred in python code, raised by c++ code, file: ["
               << file << "], function: [" << function << "], line_num:" << line_num;

}

void throw_exception_to_client(const std::string& err_msg,
                               const char* file,
                               const char* function,
                               int line_num) {
    std::string exception_path
        = google::StringFromEnv("BIGFLOW_PYTHON_EXCEPTION_TOFT_STYLE_PATH", "");
    std::string detailed_msg = err_msg + "\nThis error was occurred in c++ code in file: ["
                               + file + "], function: [" + function + "], line_num: "
                               + std::to_string(line_num);
    write_exception_to_file(exception_path, detailed_msg);
    LOG(FATAL) << "Exception occurred, err_msg = " << detailed_msg;
}

void throw_exception_to_client_without_abortion(const std::string& err_msg, bool is_serialized) {
    std::string exception_path =
        google::StringFromEnv("BIGFLOW_PYTHON_EXCEPTION_TOFT_STYLE_PATH", "");
    // todo(yexianjin): remove this hack once we don't want to support Spark 2.1.2 and below.
    if (exception_path.empty()) {
        exception_path =
            google::StringFromEnv("BIGFLOW_PYTHON_EXCEPTION_TOFT_STYLE_PATH_WITH_PASSWORD", "");
    }
    if (is_serialized) {
        write_serialized_exception_to_file(exception_path, err_msg);
    } else {
        write_exception_to_file(exception_path, err_msg);
    }
}

} // namespace python
} // namespace bigflow
} // namespace baidu

