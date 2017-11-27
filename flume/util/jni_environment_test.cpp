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
// Tests for JniEnvironment.

#include "flume/util/jni_environment.h"

#include "gtest/gtest.h"

namespace {

using baidu::flume::util::JniEnvironment;

TEST(JniEnvironmentTest, CreateVm) {
    setenv("CLASSPATH", "testdata", true);
    JniEnvironment wing_jni_env;
    JNIEnv* env = wing_jni_env.get_env();

    jclass test_class = env->FindClass("JNITest");
    ASSERT_TRUE(NULL != test_class);

    // Creates object.
    jmethodID constructor = env->GetMethodID(test_class, "<init>", "()V");
    jobject obj = env->NewObject(test_class, constructor);
    ASSERT_TRUE(NULL != obj);

    // Tests calling getIntValue().
    jmethodID get_int_method = env->GetMethodID(test_class, "getIntValue", "()I");
    ASSERT_TRUE(NULL != get_int_method);

    jint int_value = env->CallIntMethod(obj, get_int_method);
    ASSERT_EQ(0, int_value);

    // Tests calling setIntValue().
    jmethodID set_int_method = env->GetMethodID(test_class, "setIntValue", "(I)V");
    ASSERT_TRUE(NULL != set_int_method);

    env->CallObjectMethod(obj, set_int_method, 100);
    ASSERT_EQ(100, env->CallIntMethod(obj, get_int_method));

    // Tests calling getStringValue().
    jmethodID get_string_method = env->GetMethodID(test_class, "getStringValue",
                                  "()Ljava/lang/String;");
    ASSERT_TRUE(NULL != get_string_method);

    jstring string_value = static_cast<jstring>(env->CallObjectMethod(obj, get_string_method));
    ASSERT_TRUE(NULL != string_value);

    const char* str = env->GetStringUTFChars(string_value, NULL);
    ASSERT_STREQ("hello world", str);
    env->ReleaseStringUTFChars(string_value, str);

    // Tests calling setStringValue().
    jmethodID set_string_method = env->GetMethodID(test_class, "setStringValue",
                                  "(Ljava/lang/String;)V");
    ASSERT_TRUE(NULL != set_string_method);

    jstring new_string_value = env->NewStringUTF("hello jni");
    env->CallObjectMethod(obj, set_string_method, new_string_value);

    jstring another_new_string_value =
        static_cast<jstring>(env->CallObjectMethod(obj, get_string_method));
    const char* new_str = env->GetStringUTFChars(another_new_string_value, NULL);
    ASSERT_STREQ("hello jni", new_str);
    env->ReleaseStringUTFChars(another_new_string_value, new_str);
}

TEST(JniEnvironmentTest, MultipleEnv) {
    setenv("CLASSPATH", "testdata", true);
    JniEnvironment wing_jni_env;
    JNIEnv* env = wing_jni_env.get_env();

    jclass test_class = env->FindClass("JNITest");
    ASSERT_TRUE(NULL != test_class);

    JniEnvironment another_wing_jni_env;
    JNIEnv* another_env = wing_jni_env.get_env();

    test_class = another_env->FindClass("JNITest");
    ASSERT_TRUE(NULL != test_class);
}

}  // namespace
