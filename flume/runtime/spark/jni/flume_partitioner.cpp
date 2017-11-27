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
// Author: Wang Cong <bigflow-opensource@baidu.com>

#include "flume/runtime/spark/jni/flume_partitioner__.h"

#include "glog/logging.h"

#include "flume/runtime/spark/shuffle_protocol.h"

using baidu::flume::runtime::spark::ShuffleHeader;

JNIEXPORT jint JNICALL Java_com_baidu_flume_runtime_spark_impl_FlumePartitioner_00024_jniGetPartition
  (JNIEnv* env, jobject ignored, jbyteArray key) {
    jsize key_size = env->GetArrayLength(key);
    char* key_bytes =
        reinterpret_cast<char*>(env->GetByteArrayElements(key, NULL));

    ShuffleHeader* header = reinterpret_cast<ShuffleHeader*>(key_bytes);
    DCHECK_LE(sizeof(*header), key_size);
    jint partition = header->partition();

    env->ReleaseByteArrayElements(key, reinterpret_cast<jbyte*>(key_bytes), JNI_ABORT);
    return partition;
}

