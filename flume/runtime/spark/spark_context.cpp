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

#include "flume/runtime/spark/spark_context.h"

#include "glog/logging.h"

#include "flume/proto/logical_plan.pb.h"
#include "flume/proto/physical_plan.pb.h"
#include "flume/util/jni_environment.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

static const char* SPARK_CONTEXT_CLASS = "org/apache/spark/SparkContext";
static const char* SPARK_CONTEXT_CONSTRUCTOR_PARAM = "(Lorg/apache/spark/SparkConf;)V";

static const char* SPARK_CONF_CLASS = "org/apache/spark/SparkConf";
static const char* SPARK_CONF_CONSTRUCTOR_PARAM = "(Z)V";
static const char* SPARK_CONF_SET_METHOD = "set";
static const char* SPARK_CONF_SET_METHOD_PARAM =
    "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/spark/SparkConf;";

static const char* SPARK_JOB_COMPANION_CLASS = "com/baidu/flume/runtime/spark/SparkJob$";
static const char* SPARK_JOB_COMPANION_CONSTRUCTOR_PARAM = "()V";
static const char* SPARK_JOB_COMPANION_RUN_METHOD = "run";
static const char* SPARK_JOB_COMPANION_RUN_METHOD_PARAM = "(Lorg/apache/spark/SparkContext;[B)Z";

using util::JniEnvironment;

class SparkContext::Impl {
public:
    Impl(const PbJobConfig& config) : _config(config) {
        JniEnvironment jni_environment;
        _env = jni_environment.get_env();

        /* Construct SparkConf */
        jclass j_spark_conf_class = _env->FindClass(SPARK_CONF_CLASS);
        JniEnvironment::check_and_describe(_env, j_spark_conf_class);

        jmethodID j_spark_conf_constructor = _env->GetMethodID(
                j_spark_conf_class,
                "<init>",
                SPARK_CONF_CONSTRUCTOR_PARAM);

        _j_spark_conf = _env->NewObject(
                j_spark_conf_class,
                j_spark_conf_constructor,
                /*loadDefaults=*/true);

        LOG(INFO) << "SparkConf constructed";

        /* Set SparkConf */
        _j_spark_conf_set_method = _env->GetMethodID(
                j_spark_conf_class,
                SPARK_CONF_SET_METHOD,
                SPARK_CONF_SET_METHOD_PARAM);

        for (int i = 0; i < config.kv_config_size(); ++i) {
            const std::string& key = config.kv_config(i).key();
            const std::string& value = config.kv_config(i).value();

            set(key, value);

            LOG(INFO) << "Set key: " << key << ", value: " << value;
        }

        /* Construct SparkContext from SparkConf */
        jclass j_spark_context_class = _env->FindClass(SPARK_CONTEXT_CLASS);
        JniEnvironment::check_and_describe(_env, j_spark_context_class);
        LOG(INFO) << "Get SparkContext class";

        jmethodID j_spark_context_constructor = _env->GetMethodID(
                j_spark_context_class,
                "<init>",
                SPARK_CONTEXT_CONSTRUCTOR_PARAM);
        JniEnvironment::check_and_describe(_env, j_spark_context_constructor);
        LOG(INFO) << "Get SparkContext constructor";

        _j_spark_context = _env->NewObject(
                j_spark_context_class,
                j_spark_context_constructor,
                _j_spark_conf);
        JniEnvironment::check_and_describe(_env, _j_spark_context);
        LOG(INFO) << "SparkContext constructed";
    }

    virtual ~Impl() {
        _env->DeleteLocalRef(_j_spark_context);
        _env->DeleteLocalRef(_j_spark_conf);
        _env->DeleteLocalRef(_j_spark_job_companion);
    }

    bool run_job(const PbJob& job) {
        /* Construct SparkJob comapnion object */
        jclass j_spark_job_companion_class = _env->FindClass(SPARK_JOB_COMPANION_CLASS);
        JniEnvironment::check_and_describe(_env, j_spark_job_companion_class);
        LOG(INFO) << "Dont creating SparkJob";

        jmethodID j_spark_job_companion_constructor = _env->GetMethodID(
                j_spark_job_companion_class,
                "<init>",
                SPARK_JOB_COMPANION_CONSTRUCTOR_PARAM);
        JniEnvironment::check_and_describe(_env, j_spark_job_companion_constructor);

        _j_spark_job_companion = _env->NewObject(
                j_spark_job_companion_class,
                j_spark_job_companion_constructor);
        JniEnvironment::check_and_describe(_env, _j_spark_job_companion);

        _j_spark_job_companion_run_method = _env->GetMethodID(
                j_spark_job_companion_class,
                SPARK_JOB_COMPANION_RUN_METHOD,
                SPARK_JOB_COMPANION_RUN_METHOD_PARAM);
        JniEnvironment::check_and_describe(_env, _j_spark_job_companion_run_method);

        std::string job_string = job.SerializeAsString();
        jsize len = job_string.length();
        jbyteArray j_job_byte_array = _env->NewByteArray(len);
        _env->SetByteArrayRegion(
                j_job_byte_array,
                0u,
                len,
                reinterpret_cast<jbyte*>(const_cast<char*>(job_string.data())));

        jboolean ret = _env->CallBooleanMethod(
                _j_spark_job_companion,
                _j_spark_job_companion_run_method,
                _j_spark_context,
                j_job_byte_array);
        JniEnvironment::check_and_describe(_env);

        _env->DeleteLocalRef(j_job_byte_array);
        return ret;
    }

    bool kill() {}

    std::string job_id() { return ""; }

private:
    void set(const std::string& key, const std::string& value) {
        JniEnvironment::check_and_describe(_env, _j_spark_conf);
        JniEnvironment::check_and_describe(_env, _j_spark_conf_set_method);

        LOG(INFO) << "Setting key: " << key << ", value: " << value;
        jstring key_string = _env->NewStringUTF(key.c_str());
        jstring value_string = _env->NewStringUTF(value.c_str());

        jobject ret = _env->CallObjectMethod(
                _j_spark_conf,
                _j_spark_conf_set_method,
                key_string,
                value_string);

        LOG(INFO) << "CallObj called";
        JniEnvironment::check_and_describe(_env, ret);
        LOG(INFO) << "Done setting";
        _env->DeleteLocalRef(key_string);
        _env->DeleteLocalRef(value_string);
    }

private:
    PbJobConfig _config;

    JNIEnv* _env;

    jobject _j_spark_context;
    jobject _j_spark_conf;
    jobject _j_spark_job_companion;

    jmethodID _j_spark_job_companion_run_method;
    jmethodID _j_spark_conf_set_method;
};

SparkContext::SparkContext(const PbJobConfig& config)
    : _impl(new SparkContext::Impl(config)) {
}

SparkContext::~SparkContext() {
}

bool SparkContext::run_job(const PbJob& job) {
    return _impl->run_job(job);
}

void SparkContext::submit_job(const PbJob& job) {
    LOG(FATAL) << "Not implemented.";
    // _impl->submit_job(job);
}

void SparkContext::kill_job() {
    _impl->kill();
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
