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
// Author: Zheng Gonglin <zhenggonglin@baidu.com>

#include <string>

#include "flume/runtime/spark/jni/flume_filter_before_repartition__.h"

#include "glog/logging.h"

#include "flume/runtime/spark/shuffle_protocol.h"

using baidu::flume::runtime::spark::ShuffleHeader;

JNIEXPORT jboolean JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_FlumeFilterBeforeRepartition_00024_jniFilteRDDData
  (JNIEnv *env, jobject ignored, jint rdd_index, jintArray task_indices_array, jbyteArray key, jbyteArray value) {
    jsize key_size = env->GetArrayLength(key);
    char* key_bytes =
        reinterpret_cast<char*>(env->GetByteArrayElements(key, NULL));

    ShuffleHeader* header = reinterpret_cast<ShuffleHeader*>(key_bytes);
    DCHECK_LE(sizeof(*header), key_size);

    uint32_t task_index = header->task_index();

    jsize task_indices_array_size = env->GetArrayLength(task_indices_array);
    int* int_array = reinterpret_cast<int*>(env->GetIntArrayElements(task_indices_array, NULL));

    bool retain = false;
    for (int i = 0; i < task_indices_array_size; i++) {
        if (task_index == (uint32_t)int_array[i]) {
            retain = true;
            break;
        }
    }
    env->ReleaseByteArrayElements(key, reinterpret_cast<jbyte*>(key_bytes), JNI_ABORT);

    return retain;
}

