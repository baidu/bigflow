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
// Author: Wang Cong (bigflow-opensource@baidu.com)
//

#ifndef BIGFLOW_PYTHON_PYTHON_INTERPRETER_H
#define BIGFLOW_PYTHON_PYTHON_INTERPRETER_H

#include <cstdio>
#include <iostream>

#include "boost/python/object_fwd.hpp"
#include "boost/serialization/singleton.hpp"
#include "glog/logging.h"
#include "toft/base/singleton.h"
#include "toft/base/scoped_array.h"

// #include "flume/util/singleton.h"

namespace baidu {
namespace bigflow {
namespace python {

/* A singleton that wraps CPython Interpreter. */
class PythonInterpreter : public toft::SingletonBase<PythonInterpreter> {
    friend class toft::SingletonBase<PythonInterpreter>;

public:
    PythonInterpreter();
    virtual ~PythonInterpreter();

public:
    typedef void (*ExceptionHandlerFunc)(const char* file, const char* function, int line_num);
    typedef void (*ExceptionHandlerWithMsgFunc)(const std::string&,
                                                const char* file,
                                                const char* function,
                                                int line_num);

    void set_exception_handler(ExceptionHandlerFunc handler);

    const void handle_exception(const char* file,
                                const char* function,
                                int line_num);

    void set_exception_handler_with_error_msg(ExceptionHandlerWithMsgFunc handler_with_msg);

    const void handle_exception_with_error_msg(const std::string& err_msg,
                                               const char* file,
                                               const char* function,
                                               int line_num);


    void initialize_interpreter();
    bool is_interpreter_initialized();
    void finalize_interpreter();

private:
    void set_python_home();
    void __set_python_home(const std::string& path);
    void apply_initialize_hooks();
    void apply_finalize_hooks();
    void apply(const std::string& file_name);

private:
    toft::scoped_array<char> _python_home_chars;
    toft::scoped_array<char> _python_prog_chars;
    toft::scoped_array<char> _python_path_chars;

    ExceptionHandlerFunc _handler;
    ExceptionHandlerWithMsgFunc _handler_with_error_msg;
};

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

namespace boost {
namespace python {

// for gtest output
std::ostream& operator<<(std::ostream& out, const boost::python::object& obj);

}  // namespace python
}  // namespace boost

#define BIGFLOW_HANDLE_PYTHON_EXCEPTION() \
    PythonInterpreter::Instance()->handle_exception(__FILE__, __PRETTY_FUNCTION__, __LINE__)

#define BIGFLOW_HANDLE_PYTHON_EXCEPTION_WITH_ERROR_MESSAGE(ERR_MSG) \
    PythonInterpreter::Instance()->handle_exception_with_error_msg(ERR_MSG,  __FILE__, __PRETTY_FUNCTION__, __LINE__)


#define BIGFLOW_PYTHON_RAISE_EXCEPTION(ERR_MSG) BIGFLOW_HANDLE_PYTHON_EXCEPTION_WITH_ERROR_MESSAGE(ERR_MSG)

#define BIGFLOW_PYTHON_RAISE_EXCEPTION_IF(condition, msg) do{ \
    if (condition) { \
        std::string errmsg = "Condition [" + std::string(#condition) + "] return true, so raise exception\n" + msg; \
        BIGFLOW_HANDLE_PYTHON_EXCEPTION_WITH_ERROR_MESSAGE(errmsg); \
    } \
}while(0)

#endif //  BIGFLOW_PYTHON_PYTHON_INTERPRETER_H
