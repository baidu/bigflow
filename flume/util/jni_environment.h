/***************************************************************************
 *
 * Copyright (c) 2014 Baidu, Inc. All Rights Reserved.
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
// Author: Chen Chi <chenchi01@baidu.com>
// Helper class for initializing and destroying jvm environment.

#ifndef FLUME_UTIL_JNI_ENVIRONMENT_H_
#define FLUME_UTIL_JNI_ENVIRONMENT_H_

#include <string>

#include "jni.h"  // NOLINT

#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace util {

class JniEnvironment {
public:
    // If there is a running jvm, then this function gets the env of the existing jvm;
    // If there is no running jvm, then this function creates a new jvm with the specified class
    // path (environment variable CLASSPATH).
    // get_env() will return NULL if any error happens during getting or creating jvm.
    JniEnvironment();

    JNIEnv* get_env() const;

    static void call_method(const std::string& clazz_name, const std::string& method_name,
                            const std::string& signature, const std::string& in, std::string* out);

    template<typename JTYPE>
    static void check_and_describe(JNIEnv* env, JTYPE object) {
        if (reinterpret_cast<JTYPE>(NULL) == object) {
            jthrowable exception = env->ExceptionOccurred();

            if (exception != reinterpret_cast<jthrowable>(NULL)) {
                env->ExceptionDescribe();
            }
            LOG(FATAL) << "Error getting JVM object";
        }
    }

    static void check_and_describe(JNIEnv* env) {
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            LOG(FATAL) << "Erroring calling method, see errors below";
        }
    }

private:
    JNIEnv* _env;
};

}  // namespace util
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_UTIL_JNI_ENVIRONMENT_H_
