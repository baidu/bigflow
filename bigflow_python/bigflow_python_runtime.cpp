/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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
// Maintainer: Ye, Xianjin(bigflow-opensource@baidu.com)
//
#include <dlfcn.h>
#include "toft/storage/path/path.h"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "flume/util/jni_environment.h"
#include "flume/runtime/spark/jni_util/java_exception_thrower.h"

#include "bigflow_python/common/python.h"
#include "bigflow_python/python_interpreter.h"
#include "flume/runtime/spark/spark_task_env.h"

DECLARE_string(flume_backend);

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

static const char* PYTHON_HOME = "__bigflow_on_spark__/python_runtime";
static const char* APPLICATION_BASE = "__bigflow_on_spark_application__";

static const char* JVM_BIGFLOW_RUNTIME_EXCEPTION = "com/baidu/flume/runtime/BigflowRuntimeException";

namespace baidu {
namespace bigflow {
namespace python {

void setup_python_and_application_path() {
    // bigflow_on_spark.tgz will treated as a cache archive on yarn mode, it's extracted to
    // 'pwd'/__bigflow_on_spark__
    const std::string python_home = toft::Path::Join(toft::Path::GetCwd(), PYTHON_HOME);
    const std::string application_home = toft::Path::Join(toft::Path::GetCwd(), APPLICATION_BASE);
    FLAGS_flume_backend = "spark";
    flume::runtime::spark::FLAGS_flume_application_home = application_home;
    flume::runtime::spark::FLAGS_flume_python_home = python_home;

}

inline void jni_throw_bigflow_runtime_exception(const std::string& exception_msg,
                                                const char* file,
                                                const char* function,
                                                int line_num) {
    using flume::util::JniEnvironment;
    JniEnvironment jni_environment;
    JNIEnv* env = jni_environment.get_env();

    throw baidu::flume::runtime::spark::JavaExceptionThrower(env,
                                                             JVM_BIGFLOW_RUNTIME_EXCEPTION,
                                                             exception_msg, file,
                                                             line_num);
}

void raise_exception(const char* file, const char* function, int line_num) {
    boost::python::object error = get_formatted_exception();
    // we need to serialize exception here as get_formatted_exception should be called only once.
    // get_formatted_exception use PyErr_Fetch to retrieve error indicator which would be cleared
    // after fetching.
    boost::python::object cloudpickle = boost::python::import("bigflow.core.serde.cloudpickle");
    boost::python::object dumped = cloudpickle.attr("dumps")(error);
    std::string dumped_str = boost::python::extract<std::string>(dumped);

    throw_exception_to_client_without_abortion(dumped_str, true);
    std::string error_str = boost::python::extract<std::string>(boost::python::str(error));

    // TODO(wangcong09|yexianjin): Is there any other way to reset interpreter?
    PythonInterpreter::Instance()->finalize_interpreter();
    PythonInterpreter::Instance()->initialize_interpreter();
    std::string exception_msg = "Error raised from Python Code, details(s):\n" + error_str;
    jni_throw_bigflow_runtime_exception(exception_msg, file, function, line_num);

}

void raise_exception_with_msg(const std::string& error_msg,
                     const char* file,
                     const char* function,
                     int line_num) {
    throw_exception_to_client_without_abortion(error_msg, false);

    // TODO(wangcong09|yexianjin): Is there any other way to reset interpreter?
    PythonInterpreter::Instance()->finalize_interpreter();
    PythonInterpreter::Instance()->initialize_interpreter();
    std::string exception_msg = "Error raised from BigFlow, detail(s):\n" + error_msg;
    jni_throw_bigflow_runtime_exception(exception_msg, file, function, line_num);
}

void _py_initialize() {


    setup_python_and_application_path();
    // on per task build. by miaodongdong
    if(!PythonInterpreter::Instance()->is_interpreter_initialized()){
        PythonInterpreter::Instance()->initialize_interpreter();
    }
    PythonInterpreter::Instance()->set_exception_handler(&raise_exception);
    PythonInterpreter::Instance()->set_exception_handler_with_error_msg(&raise_exception_with_msg);


    /**
     * Once the following two functions are called
     * Every Python functions must be called within PyGILState_Ensure and PyGILState_Release;
     * or crash happens.
     */

    // PyEval_InitThreads();
    // PyEval_ReleaseLock ();
}

bool py_initialize(){
    // todo(yexianjin): remove this LD_PRELOAD unset procedure, the only reason it exists is that
    // we want to preload jemalloc to detect memory leak. But the preload jemalloc causes subprocess
    // fails to start.
    std::cerr << "Unset LD_PRELOAD" << std::endl;
    if(unsetenv("LD_PRELOAD") != 0U) {
        std::cerr << "unsetenv failed";
    }

    // Make libpython.so a global object so that some of modules linked against libpython.so works
    std::cerr << "Open libpython2.7.so globally..." << std::endl;
    if(dlopen("libpython2.7.so.1.0", RTLD_LAZY | RTLD_GLOBAL) ==  NULL) {
        std::cerr << "dlopen failed, errno = " << errno << std::endl;
    }
    std::cerr << "Done opening libpython2.7.so..." << std::endl;

    FlumeSparkTaskEnv::setup_callback = _py_initialize;
}

static bool _force_initialized_python_interpreter = py_initialize();

}  // namespace python
}  // namespace bigflow
}  // namespace baidu

