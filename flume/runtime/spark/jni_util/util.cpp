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

#include "flume/runtime/spark/jni_util/java_exception_thrower.h"

#include "util.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

void ConvertException(JNIEnv *env, const char *file, int line) {
    std::ostringstream ss;
    try {
        throw;
    }
    catch (JavaExceptionThrower& e) {
        e.throw_java_exception(env);
    }
    catch (std::bad_alloc& e) {
        ss << e.what() << " in " << file << " line " << line;
        ThrowException(env, OutOfMemory, ss.str());
    }
    catch (std::exception& e) {
        ss << e.what() << " in " << file << " line " << line;
        ThrowException(env, FatalError, ss.str());
    }
}

void ThrowException(JNIEnv *env, ExceptionKind exception, const std::string &msg_str) {
    std::string message;
    jclass exception_class = NULL;

    switch (exception) {
        case OutOfMemory:
            exception_class = env->FindClass("java/lang/OutOfMemoryError");
            message = msg_str;
            break;
        case FatalError:
        case RuntimeError:
            exception_class = env->FindClass("com/baidu/flume/runtime/BigflowRuntimeException");
            message = msg_str;
            break;

        // should never reach here
        case ExceptionKindMax:
        default:
            break;
    }

    if (exception_class != NULL) {
        env->ThrowNew(exception_class, message.c_str());
    }
}


} // spark
} // runtime
} // flume
} //