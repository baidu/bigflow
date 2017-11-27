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

// Most of these functions wrap the unchecked versions of CPython API for efficiency and convience.
// They will be faster than the corresponding methods in the library of boost::python.

#ifndef BIGFLOW_PYTHON_COMMON_PYTHON_H
#define BIGFLOW_PYTHON_COMMON_PYTHON_H

#include "boost/python.hpp"
#include "bigflow_python/common/side_input.h"
#include <memory>
#include <vector>
#include <flume/core/iterator.h>
#include "toft/base/string/string_piece.h"

namespace baidu {
namespace bigflow {
namespace python {

#define SafeDec(buffer_size, result) (buffer_size > result ? buffer_size - result : 0)

boost::python::object tuple_get_item(const boost::python::object& tuple, int n);

// set item to tuple.
// Note: since tuple is not supposed to be modified,
// the python interpreter has some limitations of modifying a tuple,
// if a tuple's reference count is not 1, do not call this method,
// or you'll meet an undefined behaviour, and the program may crash in some weird places.
void tuple_set_item(const boost::python::object& tuple, int n, const boost::python::object& object);

boost::python::object list_get_item(const boost::python::object& list, int n);

boost::python::object object_get_item(const boost::python::object& obj, int n);

void list_set_item(const boost::python::object& list, int n, const boost::python::object& object);

void list_append_item(const boost::python::object& list,  const boost::python::object& object);

boost::python::tuple new_tuple(int n);

int64_t list_len(const boost::python::object& list);

int64_t tuple_len(const boost::python::object& tuple);

int64_t object_len(const boost::python::object& obj);

void dict_set_item(boost::python::object& dict,
        const boost::python::object& key,
        const boost::python::object& value);

boost::python::tuple construct_args(int normal_arg,
    const std::vector<boost::python::object>& side_input);

void fill_args(int normal_arg,
    const std::vector<boost::python::object>& side_input,
    boost::python::tuple* ret);

bool is_list(const boost::python::object& object);

template<class F>
int list_foreach(const boost::python::object& list, F f) {
    int length = boost::python::len(list);
    for (int i = 0; i < length; i++) {
        boost::python::object item = list_get_item(list, i);
        f(item);
    }
    return length;
}

template<class F>
int vec_foreach(const std::vector<boost::python::object>& vec, F f) {
    int length = vec.size();
    for (int i = 0; i < length; i++) {
        f(vec[i]);
    }
    return length;
}


// convert a side input to a list
boost::python::object side_input_list(const boost::python::object& side_input);

// convert a side input to a vector, so that we can iterate the side input in C++ more quickly.
const std::vector<boost::python::object>& side_input_vec(const boost::python::object& side_input);

bool is_dict(const boost::python::object& object);

bool is_tuple(const boost::python::object& object);

bool is_int(const boost::python::object& object);

inline bool is_python_bool(const boost::python::object& object) {
    return PyBool_Check(object.ptr());
}

inline bool is_python_long(const boost::python::object& object) {
    return PyLong_CheckExact(object.ptr());
}

inline bool is_python_string(const boost::python::object& object) {
    return PyString_CheckExact(object.ptr());
}

inline bool is_python_unicode(const boost::python::object& object) {
    return PyUnicode_CheckExact(object.ptr());
}

inline bool is_python_float(const boost::python::object& object) {
    return PyFloat_CheckExact(object.ptr());
}

// get buffer from a str. Do not modify the buffer in the returned value.
toft::StringPiece str_get_buf(const boost::python::object& str);

boost::python::object tuple_get_slice(const boost::python::object& tp, int start, int end);

boost::python::object list_get_slice(const boost::python::object& tp, int start, int end);

boost::python::object object_get_slice(const boost::python::object& tp, int start, int end);

uint32_t python_long_as_buffer(PyLongObject* v, char *buffer, uint32_t buffer_size, bool reverse);

boost::python::object get_origin_serde(const boost::python::object& serde);

std::string get_entity_reflect_str(const boost::python::object& entity);

std::string get_entity_config_str(const boost::python::object& entity);

boost::python::object deep_copy(const boost::python::object& obj);

std::string format_exception();

// get type name of obj, returned a C++ string
std::string type_string(const boost::python::object& obj);

// retrieve the raised python exception and formatted it.
// Note this method will clear the exception in the python interpreter.
boost::python::object get_formatted_exception();

// write the raised python exception to file
// This method will clear the exception in the python interpreter.
void write_exception_to_file(const std::string& file);

// Throw the raised python excpetion to client side.
void throw_exception_to_client(const char* file, const char* function, int line_num);

// Construct a BigflowRuntimeException with a specific error message and throw it to client side.
// This method will abort the execution of current remote process.
void throw_exception_to_client(const std::string& err_msg,
                               const char* file, const char* function, int line_num);
// Construct a BigflowRuntimeException with a specific error message and throw it to client side.
// But this method will not abort the execution of remote process.
void throw_exception_to_client_without_abortion(const std::string& err_msg, bool is_serialized);

} // namespace python
} // namespace bigflow
} // namespace baidu

#endif // #ifndef BIGFLOW_PYTHON_COMMON_PYTHON_H
