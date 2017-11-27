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
// Author: Ye, Xianjin(bigflow-opensource@baidu.com)

#include <sstream>
#include "java_exception_thrower.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

JavaExceptionThrower::JavaExceptionThrower(const char *file_path, int line_num):
    std::runtime_error("Java exception has been occurred, "
                       "Terminate JNI by throwing a c++ exception"),
    m_class_name(),
    m_file_path(file_path),
    m_line_num(line_num) {}

JavaExceptionThrower::JavaExceptionThrower(JNIEnv *env, const char *class_name, std::string message,
                                           const char *file_path, int line_num):
    std::runtime_error(message),
    m_class_name(class_name),
    m_file_path(file_path),
    m_line_num(line_num) {}


void JavaExceptionThrower::throw_java_exception(JNIEnv *env) {
    jclass runtime_exception = env->FindClass(m_class_name);
    if (env->ExceptionCheck()) {
        return;
    }

    std::stringstream ss;
    ss << what() << std::endl << "(" << m_file_path << ":" << m_line_num << ")";
    env->ThrowNew(runtime_exception, ss.str().c_str());
}

void JavaExceptionThrower::terminate_jni_if_java_exception_occurred(JNIEnv *env,
                                                                    CleanUpFunction clean_up_func,
                                                                    const char *file_path,
                                                                    int line_num) {
    if (!env->ExceptionCheck()) {
        return;
    }

    if (clean_up_func) {
        clean_up_func();
    }

    throw JavaExceptionThrower(file_path, line_num);

}

} // spark
} // runtime
} // flume
} // baidu
