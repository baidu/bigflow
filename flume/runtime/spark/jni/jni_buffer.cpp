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
// Author: Wang Cong <wangcong09@baidu.com>

#include "flume/runtime/spark/jni/jni_buffer__.h"

#include "flume/runtime/spark/kv_buffer.h"
#include "flume/util/jni_environment.h"

using baidu::flume::runtime::spark::KVBuffer;
using baidu::flume::util::JniEnvironment;

JNIEXPORT void JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_KVBuffer_00024_jniReset
    (JNIEnv* env, jobject clazz, jlong ptr) {
    KVBuffer* buffer = reinterpret_cast<KVBuffer*>(ptr);
    buffer->reset();
}

JNIEXPORT void JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_KVBuffer_00024_jniNext
    (JNIEnv* env, jobject clazz, jlong ptr) {
    KVBuffer* buffer = reinterpret_cast<KVBuffer*>(ptr);
    buffer->next();
}

JNIEXPORT jboolean JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_KVBuffer_00024_jniHasNext
    (JNIEnv* env, jobject clazz, jlong ptr) {
    KVBuffer* buffer = reinterpret_cast<KVBuffer*>(ptr);
    return buffer->has_next();
}

JNIEXPORT jbyteArray JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_KVBuffer_00024_jniKey
    (JNIEnv* env, jobject clazz, jlong ptr) {
    KVBuffer* buffer = reinterpret_cast<KVBuffer*>(ptr);
    toft::StringPiece key;
    buffer->key(&key);
    jbyteArray j_bytes = env->NewByteArray(key.size());
    JniEnvironment::check_and_describe(env, j_bytes);
    env->SetByteArrayRegion(j_bytes, 0, key.size(), (const jbyte*)key.data());
    return j_bytes;
}

JNIEXPORT jbyteArray JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_KVBuffer_00024_jniValue
    (JNIEnv* env, jobject clazz, jlong ptr) {
    KVBuffer* buffer = reinterpret_cast<KVBuffer*>(ptr);
    toft::StringPiece value;
    buffer->value(&value);
    jbyteArray j_bytes = env->NewByteArray(value.size());
    JniEnvironment::check_and_describe(env, j_bytes);
    env->SetByteArrayRegion(j_bytes, 0, value.size(), (const jbyte*)value.data());
    return j_bytes;
}

