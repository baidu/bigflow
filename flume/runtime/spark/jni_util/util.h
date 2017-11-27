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

#ifndef FLUME_RUNTIME_SPARK_JNI_UTIL_UTIL
#define FLUME_RUNTIME_SPARK_JNI_UTIL_UTIL

#include <jni.h>
namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

#define CATCH_STD()                                                                                \
    catch (...)                                                                                    \
    {                                                                                              \
        ConvertException(env, __FILE__, __LINE__);                                                 \
    }

enum ExceptionKind {
    OutOfMemory,
    RuntimeError,
    FatalError,
    ExceptionKindMax, // always keep this as the last one
};

void ConvertException(JNIEnv* env, const char* file, int line);

void ThrowException(JNIEnv* env, ExceptionKind exception, const std::string& msg_str);

}
}
}
}



#endif //FLUME_RUNTIME_SPARK_JNI_UTIL_UTIL