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

#include "flume/runtime/spark/jni/jni_task__.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "flume/runtime/spark/jni_util/util.h"
#include "flume/runtime/spark/input_executor.h"
#include "flume/runtime/spark/kv_buffer.h"
#include "flume/runtime/spark/shuffle_input_executor.h"
#include "flume/runtime/spark/spark_task.h"
#include "flume/service/common/profiler_util.h"
#include "flume/util/jni_environment.h"
#include "flume/runtime/spark/spark_task_env.h"

#include "glog/logging.h"
#include "toft/base/string/algorithm.h"
#include "toft/storage/path/path.h"


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

using baidu::flume::util::JniEnvironment;
using baidu::flume::runtime::spark::ConvertException;
using baidu::flume::runtime::spark::SparkTask;
using baidu::flume::runtime::spark::KVBuffer;
using baidu::flume::service::ProfilerUtil;

std::string GetSparkTaskLogDirectory() {
    std::string log_directories_str = std::getenv("LOG_DIRS");
    if (log_directories_str.empty()) {
        return toft::Path::GetCwd();
    }

    std::vector<std::string> parts;
    const std::string separator = ",";
    toft::SplitStringKeepEmpty(log_directories_str, separator, &parts);

    return parts.front();
}

template<typename T>
bool ParseFromBytes(JNIEnv* env, jbyteArray buf, T* result) {
    jsize byte_size = env->GetArrayLength(buf);
    char* bytes =
            reinterpret_cast<char*>(env->GetByteArrayElements(buf, /*isCopy=*/NULL));
    CHECK_NOTNULL(bytes);
    bool ret = result->ParseFromArray(bytes, byte_size);
    env->ReleaseByteArrayElements(buf,
                                  reinterpret_cast<jbyte*>(bytes),
                                  JNI_ABORT);
    return ret;
}

JNIEXPORT jlong JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_FlumeTask_00024_jniBuildTask
  (JNIEnv* env, jobject ignored, jbyteArray bytes_pb_job, jbyteArray bytes_pb_spark_task, jint partition) {

    // by miaodongdong
    if (FlumeSparkTaskEnv::setup_callback != NULL){
        FlumeSparkTaskEnv::setup_callback();
    }

    baidu::flume::PbSparkTask pb_task;
    CHECK(ParseFromBytes(env, bytes_pb_spark_task, &pb_task));

    baidu::flume::PbSparkJob::PbSparkJobInfo pb_job;
    CHECK(ParseFromBytes(env, bytes_pb_job, &pb_job));

    try {

        SparkTask* task = new SparkTask(pb_job, pb_task, /*is_use_pipe=*/false, partition);

        std::string task_attempt_id = pb_task.runtime_task_ctx().task_attempt_id();
        LOG(INFO) << "Bigflow Spark task attempt id: " << task_attempt_id;

        //TODO: add profiling with gperftools
        //    const std::string cpu_prof_basename = "cpu." + task_attempt_id + ".prof";
        //    const std::string heap_prof_basename = "heap." + task_attempt_id + ".prof";

        std::string task_log_directory = GetSparkTaskLogDirectory();
        LOG(INFO) << "Bigflow Spark task log directory: " << task_log_directory;

        //TODO
        //ProfilerUtil::StartProfilingTask(task->do_cpu_profile(),
        //                                 task->do_heap_profile(),
        //                                 task_log_directory,
        //                                 cpu_prof_basename,
        //                                 heap_prof_basename);

        return reinterpret_cast<jlong>(task);
    } CATCH_STD()
}

JNIEXPORT void JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_FlumeTask_00024_jniReleaseTask
  (JNIEnv* env, jobject ignored, jlong ptr_task) {
    try {
        SparkTask* task = reinterpret_cast<SparkTask*>(ptr_task);
        LOG(INFO) << "Start releasing task: " << task;
        //TODO
        //bool do_cpu_profile = task->do_cpu_profile();
        //bool do_heap_profile = task->do_heap_profile();
        delete task;
        LOG(INFO) << "Done releasing task: " << task;

        const std::string task_log_directory = GetSparkTaskLogDirectory();
        //TODO
        //ProfilerUtil::StopProfilingTask(do_cpu_profile,
        //                                do_heap_profile,
        //                                task_log_directory);
    } CATCH_STD()
}

JNIEXPORT jlong JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_FlumeTask_00024_jniGetOutputBufferPtr
  (JNIEnv* env, jobject ignored, jlong ptr_task) {
    try {
        SparkTask* task = reinterpret_cast<SparkTask*>(ptr_task);
        KVBuffer* ptr_buffer = task->get_output_buffer();
        return reinterpret_cast<jlong>(ptr_buffer);
    } CATCH_STD()

};

JNIEXPORT void JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_FlumeTask_00024_jniRun
  (JNIEnv* env, jobject ignored, jlong ptr_task, jstring info) {

    try {
        SparkTask* task = reinterpret_cast<SparkTask*>(ptr_task);
        const char* chars_info = env->GetStringUTFChars(info, /*isCopy=*/NULL);
        CHECK_NOTNULL(chars_info);
        task->run(chars_info);
        env->ReleaseStringUTFChars(info, chars_info);
    } CATCH_STD()
}

JNIEXPORT void JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_FlumeTask_00024_jniProcessInput
  (JNIEnv* env,
   jobject ignored,
   jlong ptr_task,
   jbyteArray key,
   jint key_length,
   jbyteArray value,
   jint value_length) {
    SparkTask* task = reinterpret_cast<SparkTask*>(ptr_task);

    char* key_bytes =
        reinterpret_cast<char*>(env->GetByteArrayElements(key, /*isCopy=*/NULL));
    char* value_bytes =
        reinterpret_cast<char*>(env->GetByteArrayElements(value, /*isCopy=*/NULL));
    try {
        toft::StringPiece key_piece(key_bytes, key_length);
        toft::StringPiece value_piece(value_bytes, value_length);
        task->input_executor()->process_input(key_piece, value_piece);
    } CATCH_STD()

    env->ReleaseByteArrayElements(key, reinterpret_cast<jbyte*>(key_bytes), JNI_ABORT);
    env->ReleaseByteArrayElements(value, reinterpret_cast<jbyte*>(value_bytes), JNI_ABORT);
}

JNIEXPORT void JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_FlumeTask_00024_jniProcessKey
  (JNIEnv* env, jobject ignored, jlong ptr_task, jbyteArray key, jint key_length) {
    SparkTask* task = reinterpret_cast<SparkTask*>(ptr_task);

    char* key_bytes =
        reinterpret_cast<char*>(env->GetByteArrayElements(key, /*isCopy=*/NULL));
    try {
        task->shuffle_input_executor()->process_input_key(toft::StringPiece(key_bytes, key_length));
    } CATCH_STD()

    env->ReleaseByteArrayElements(key, reinterpret_cast<jbyte*>(key_bytes), JNI_ABORT);
}

JNIEXPORT void JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_FlumeTask_00024_jniProcessValue
  (JNIEnv* env, jobject ignored, jlong ptr_task, jbyteArray value, jint value_length) {
    SparkTask* task = reinterpret_cast<SparkTask*>(ptr_task);

    jsize value_size = env->GetArrayLength(value);

    char* value_bytes =
        reinterpret_cast<char*>(env->GetByteArrayElements(value, /*isCopy=*/NULL));
    try {
        task->shuffle_input_executor()
            ->process_input_value(toft::StringPiece(value_bytes, value_length));

    } CATCH_STD()

    env->ReleaseByteArrayElements(value, reinterpret_cast<jbyte*>(value_bytes), JNI_ABORT);
}

JNIEXPORT void JNICALL Java_com_baidu_flume_runtime_spark_impl_jni_FlumeTask_00024_jniInputDone
  (JNIEnv* env, jobject ignored, jlong ptr_task) {
    try {
        SparkTask* task = reinterpret_cast<SparkTask*>(ptr_task);

        task->input_done();
    } CATCH_STD()
}
