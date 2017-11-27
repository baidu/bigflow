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

#include "bigflow_python/python_interpreter.h"

#include <fstream>
#include <ios>

#include "boost/filesystem.hpp"
#include "boost/python/suite/indexing/vector_indexing_suite.hpp"
#include "boost/python/def.hpp"
#include "boost/lexical_cast.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "toft/base/string/string_piece.h"
#include "toft/base/string/algorithm.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/delegators/python_processor_delegator.h"
#include "bigflow_python/processors/processor.h"
#include "bigflow_python/serde/cpickle_serde.h"
#include "flume/core/iterator.h"
#include "flume/runtime/io/io_format.h"
#include "flume/runtime/local/flags.h"

DECLARE_string(flume_backend);
DECLARE_bool(flume_commit);
DECLARE_int32(flume_log_server_index);

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {
DECLARE_string(flume_python_home);
DECLARE_string(flume_application_home);
}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

namespace baidu {
namespace bigflow {
namespace python {

namespace {

// Translate FlumeIteratorDelegator::StopIteration to Python StopIteration exception
void translate(const FlumeIteratorDelegator::StopIteration& e) {
    // Use the Python C API to set up an exception object
    PyErr_SetNone(PyExc_StopIteration);
}

void expose() {
    boost::python::class_<toft::StringPiece>("StringPiece")
        .def("as_string", &toft::StringPiece::as_string)
        .def("data", &toft::StringPiece::data)
    ;

    boost::python::class_<std::vector<toft::StringPiece> >("VecStringPiece")
        .def(boost::python::vector_indexing_suite<std::vector<toft::StringPiece> >())
    ;

    boost::python::class_<std::vector<FlumeIteratorDelegator> >("VecIterator")
        .def(boost::python::vector_indexing_suite<std::vector<FlumeIteratorDelegator> >())
    ;

    boost::python::register_exception_translator<FlumeIteratorDelegator::StopIteration>(&translate);
    boost::python::class_<FlumeIteratorDelegator>("FlumeIterator", boost::python::no_init)
        .def("has_next", &FlumeIteratorDelegator::HasNext)
        .def("next", &FlumeIteratorDelegator::NextValue)
        .def("reset", &FlumeIteratorDelegator::Reset)
        .def("done", &FlumeIteratorDelegator::Done)
    ;

    boost::python::class_<EmitterDelegator>("FlumeEmitter")
        .def("emit", &EmitterDelegator::emit)
        .def("done", &EmitterDelegator::done)
    ;

    boost::python::class_<flume::runtime::Record>("Record")
        .def_readwrite("key", &flume::runtime::Record::key)
        .def_readwrite("value", &flume::runtime::Record::value)
    ;

    boost::python::class_<SideInput>("SideInput", boost::python::no_init)
        .def("__len__", &SideInput::length)
        .def("__iter__", &SideInput::get_iterator)
        .def("__getitem__", &SideInput::get_item)
        .def("as_list", &SideInput::as_list)
    ;
}

}  // namespace

PythonInterpreter::PythonInterpreter(): _handler(NULL) {
    // In the python code, we can check this environment var
    // to judge if it is running on the remote side. (or on the client side).
    CHECK_EQ(0, setenv("__PYTHON_IN_REMOTE_SIDE", "true", 1));
    initialize_interpreter();
    expose();
}

PythonInterpreter::~PythonInterpreter() {
    //if (is_interpreter_initialized()) {
    //    finalize_interpreter();
    //}
}

void PythonInterpreter::set_exception_handler(ExceptionHandlerFunc handler) {
    _handler = handler;
}

const void PythonInterpreter::handle_exception(const char* file,
                                               const char* function,
                                               int line_num) {
    (*_handler)(file, function, line_num);
}

void PythonInterpreter::set_exception_handler_with_error_msg(
    ExceptionHandlerWithMsgFunc handler_with_error_msg) {
    _handler_with_error_msg = handler_with_error_msg;
}

const void PythonInterpreter::handle_exception_with_error_msg(const std::string& err_msg,
                                                              const char* file,
                                                              const char* function,
                                                              int line_num) {
    _handler_with_error_msg(err_msg, file, function, line_num);
}

void PythonInterpreter::initialize_interpreter() {
    try {
        CHECK(!is_interpreter_initialized()) << "Python interpreter initialized already";
        LOG(INFO) << "Initializing Python interpreter...";
        set_python_home();
        Py_Initialize();
        CHECK(is_interpreter_initialized()) << "Python interpreter initialized failed";
        apply_initialize_hooks();
        LOG(INFO) << "Done initializing Python interpreter.";
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

bool PythonInterpreter::is_interpreter_initialized() {
    return Py_IsInitialized();
}

void PythonInterpreter::finalize_interpreter() {
    try {
        LOG(INFO) << "Finalizing Python interpreter...";
        apply_finalize_hooks();
        Py_Finalize();
        LOG(INFO) << "Done finalizing interpreter.";
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

void PythonInterpreter::set_python_home() {
    std::string python_home;
    if (FLAGS_flume_backend == "spark") {
        python_home = flume::runtime::spark::FLAGS_flume_python_home;
        // note: archive_base is wrong when doing local commit. However it doesn't affect commit
        // process. Commit process can be delegated to Spark Driver when ready, we don't have  to
        // worry this then. Otherwise, FLAGS_flume_spark_local should be defined to indicate local
        // or remote.
        std::string archive_base = flume::runtime::spark::FLAGS_flume_application_home;
        std::string protobuf_egg = archive_base + "/pythonlib/protobuf-2.5.0-py2.7.egg";
        std::vector<std::string> path_to_be_added = {".", archive_base, protobuf_egg};

        char* old_env = getenv("PYTHONPATH");
        std::string new_env = "PYTHONPATH=";
        if (old_env) { // update old PYTHONPATH
            toft::StringPiece python_path(old_env);
            std::set<std::string> path_set;
            toft::SplitStringToSet(python_path, ":", &path_set);

            for (auto path: path_to_be_added) {
                if (path_set.find(path) == path_set.end()) {
                    new_env += path + ":";
                }
            }
            LOG(INFO) << "Old PYTHONPATH: " << old_env;
            new_env += old_env;
        } else {
            new_env += toft::JoinStrings(path_to_be_added, ":");
        }

        LOG(INFO) << "Now PYTHONPATH: " << new_env;
        _python_path_chars.reset(new char[new_env.size() + 1]);
        std::strcpy(_python_path_chars.get(), new_env.c_str());
        CHECK_EQ(putenv(_python_path_chars.get()), 0u);
    }
    __set_python_home(python_home);
}

void PythonInterpreter::__set_python_home(const std::string& path) {
    if (path.empty()) {
        LOG(WARNING) << "Trying to PYTHONHOME as empty path, ignore!";
        return;
    }

    _python_home_chars.reset(new char[path.size() + 1]);
    std::strcpy(_python_home_chars.get(), path.c_str());

    LOG(INFO) << "Setting PythonHome:" << _python_home_chars.get();
    Py_SetPythonHome(_python_home_chars.get());

    std::string python_prog = path + "/bin/python";
    _python_prog_chars.reset(new char[python_prog.size() + 1]);
    std::strcpy(_python_prog_chars.get(), python_prog.c_str());

    LOG(INFO) << "Setting Python Binary:" << _python_prog_chars.get();
    Py_SetProgramName(_python_prog_chars.get());
}

void PythonInterpreter::apply_initialize_hooks() {
    apply(".init_hooks");
}

void PythonInterpreter::apply_finalize_hooks() {
    apply(".fini_hooks");
}

void PythonInterpreter::apply(const std::string& name) {
    // skip applying hooks when:
    //  commit stage(FLAGS_flume_commit is true) or local commit stage. (FLAGS_flume_local_commit is true)
    //  log service(FLAGS_flume_log_server_index != -1)
    if (FLAGS_flume_commit
        || flume::runtime::local::FLAGS_flume_local_commit) {
        return;
    }
    boost::filesystem::path file(name);
    if (!boost::filesystem::exists(file)) {
        LOG(WARNING) << "Hook file [" << name << "] does not exist";
        return;
    }
    std::ifstream in_stream(name.c_str(), std::ios::binary|std::ios::in);
    LOG(INFO) << "Done reading hook: " << name;
    std::string buffer(
            (std::istreambuf_iterator<char>(in_stream)),
            std::istreambuf_iterator<char>());
    CPickleSerde _cpickle(true);
    try {
        boost::python::object hooks = _cpickle.loads(buffer);
        for (int i = 0; i < boost::python::len(hooks); ++i) {
            LOG_FIRST_N(INFO, 1) << "Start hooks";
            boost::python::object hook = hooks[i];
            boost::python::object callable = hook[0];
            callable();
        }
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

namespace boost {
namespace python {

std::ostream& operator<<(std::ostream& out, const boost::python::object& obj) {
    std::string str = boost::python::extract<std::string>(boost::python::str(obj));
    return out << str;
}

}  // namespace python
}  // namespace boost
