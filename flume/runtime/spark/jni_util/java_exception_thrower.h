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
// Author: Ye, Xianjin(yexianjin@baidu.com)

#ifndef FLUME_RUNTIME_SPARK_JNI_UTIL_JAVA_EXCEPTION_THROWER
#define FLUME_RUNTIME_SPARK_JNI_UTIL_JAVA_EXCEPTION_THROWER


#include <stdexcept>
#include <functional>

#include <jni.h>
#include <string>


namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

#define THROW_JAVA_EXCEPTION(env, class_name, message)                                             \
    throw baidu::flume::runtime::spark::JavaExceptionThrower(env, class_name, message, __FILE__, __LINE__)

#define TERMINATE_JNI_IF_JAVA_EXCEPTION_OCCURRED(env, clean_up_func)                               \
    JavaExceptionThrower::terminate_jni_if_java_exception_occurred(env, clean_up_func, __FILE__, __LINE__)

class JavaExceptionThrower : public std::runtime_error {

public:
    using CleanUpFunction = std::function<void()>;

    JavaExceptionThrower(const char* file_path, int line_num);

    JavaExceptionThrower(JNIEnv* env, const char* class_name,
                         std::string message, const char* file_path, int line_num);

    void throw_java_exception(JNIEnv* env);

    // This method will throw a JavaExceptionThrower to terminate JNI then return to java if there
    // is an Java exception has been thrown before. clean_up_func will be called before throwing the
    // c++ exception if there is a pending java exception.
    static void terminate_jni_if_java_exception_occurred(JNIEnv* env,
                                                         CleanUpFunction clean_up_func,
                                                         const char* file_path,
                                                         int line_num);

private:
    const char* m_class_name;
    const char* m_file_path;
    int m_line_num;
};

} // spark
} // runtime
} // flume
} // baidu

#endif //FLUME_RUNTIME_SPARK_JNI_UTIL_JAVA_EXCEPTION_THROWER
