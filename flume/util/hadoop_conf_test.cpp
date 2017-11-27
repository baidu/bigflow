/***************************************************************************
 *
 * Copyright (c) 2013 Baidu, Inc. All Rights Reserved.
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
// Author: Zhang Yuncong (zhangyuncong@baidu.com)
//
// Description: Flume hadoop conf util test
#include <stdlib.h>

#include "flume/util/hadoop_conf.h"

#include "gtest/gtest.h"

namespace baidu {
namespace flume {
namespace util {

TEST(TestHadoopConf, TestFromFile) {
    // backup & clear sys env
    char* env_ugi = getenv(kHadoopJobUgi);
    char* env_fs_name = getenv(kFsDefaultName);
    unsetenv(kHadoopJobUgi);
    unsetenv(kFsDefaultName);

    HadoopConf conf("testdata/hadoop-site.xml");
    ASSERT_EQ("testdata/hadoop-site.xml", conf.hadoop_conf_path());
    ASSERT_EQ("test,test", conf.hadoop_job_ugi());
    ASSERT_EQ("hdfs://baidu.com:1234", conf.fs_defaultfs());

    // restore sys env if necessary
    if (env_ugi != NULL) {
        setenv(kHadoopJobUgi, env_ugi, 1);
    }
    if (env_fs_name != NULL) {
        setenv(kFsDefaultName, env_fs_name, 1);
    }
}

TEST(TestHadoopConf, TestFromEnv) {
    // backup & set sys env
    char* env_ugi = getenv(kHadoopJobUgi);
    char* env_fs_name = getenv(kFsDefaultName);

    const char* kExpectUgi = "env,env";
    const char* kExpectFsName = "hdfs:///env.com:37213";
    setenv(kHadoopJobUgi, kExpectUgi, 1);
    setenv(kFsDefaultName, kExpectFsName, 1);

    HadoopConf conf("testdata/hadoop-site.xml");
    ASSERT_EQ("testdata/hadoop-site.xml", conf.hadoop_conf_path());
    ASSERT_EQ(kExpectUgi, conf.hadoop_job_ugi());
    ASSERT_EQ(kExpectFsName, conf.fs_defaultfs());

    // restore sys env
    if (env_ugi != NULL) {
        setenv(kHadoopJobUgi, env_ugi, 1);
    } else {
        unsetenv(kHadoopJobUgi);
    }
    if (env_fs_name != NULL) {
        setenv(kFsDefaultName, env_fs_name, 1);
    } else {
        unsetenv(kFsDefaultName);
    }
}

TEST(TestHadoopConf, TestFromMap) {
    const char* kExpectUgi = "app,app";
    const char* kExpectFsName = "hdfs://app.com:34275";

    HadoopConf::JobConfMap conf_map;
    conf_map[kHadoopJobUgi] = kExpectUgi;
    conf_map[kFsDefaultName] = kExpectFsName;
    HadoopConf conf("testdata/hadoop-site.xml", conf_map);
    ASSERT_EQ("testdata/hadoop-site.xml", conf.hadoop_conf_path());
    ASSERT_EQ(kExpectUgi, conf.hadoop_job_ugi());
    ASSERT_EQ(kExpectFsName, conf.fs_defaultfs());
}

TEST(TestHadoopConf, TestPartialOverwrite1) {
    // get fs name from conf file
    // get ugi from conf map

    char* env_fs_name = getenv(kFsDefaultName);
    unsetenv(kFsDefaultName);

    const char* kExpectUgi = "app,app";

    HadoopConf::JobConfMap conf_map;
    conf_map[kHadoopJobUgi] = kExpectUgi;

    HadoopConf conf("testdata/hadoop-site.xml", conf_map);
    ASSERT_EQ("testdata/hadoop-site.xml", conf.hadoop_conf_path());
    ASSERT_EQ(kExpectUgi, conf.hadoop_job_ugi());
    ASSERT_EQ("hdfs://baidu.com:1234", conf.fs_defaultfs());

    if (env_fs_name != NULL) {
        setenv(kFsDefaultName, env_fs_name, 1);
    }
}

TEST(TestHadoopConf, TestPartialOverwrite2) {
    // get fs name from env
    // get ugi from conf map
    char* env_ugi = getenv(kHadoopJobUgi);
    char* env_fs_name = getenv(kFsDefaultName);

    const char* kExpectUgi = "app,app";
    const char* kExpectFsName = "hdfs:///env.com:37213";
    setenv(kHadoopJobUgi, "ignore.me", 1);
    setenv(kFsDefaultName, kExpectFsName, 1);

    HadoopConf::JobConfMap conf_map;
    conf_map[kHadoopJobUgi] = kExpectUgi;
    HadoopConf conf("testdata/hadoop-site.xml", conf_map);
    ASSERT_EQ("testdata/hadoop-site.xml", conf.hadoop_conf_path());
    ASSERT_EQ(kExpectUgi, conf.hadoop_job_ugi());
    ASSERT_EQ(kExpectFsName, conf.fs_defaultfs());

    if (env_ugi != NULL) {
        setenv(kHadoopJobUgi, env_ugi, 1);
    } else {
        unsetenv(kHadoopJobUgi);
    }
    if (env_fs_name != NULL) {
        setenv(kFsDefaultName, env_fs_name, 1);
    } else {
        unsetenv(kFsDefaultName);
    }
}

TEST(TestHadoopConf, TestMapOverwriteAll) {
    // get both fs name and ugi from map
    char* env_ugi = getenv(kHadoopJobUgi);
    char* env_fs_name = getenv(kFsDefaultName);

    const char* kExpectUgi = "app,app";
    const char* kExpectFsName = "hdfs:///env.com:37213";
    setenv(kHadoopJobUgi, "ignore.me", 1);
    setenv(kFsDefaultName, "hdfs:///ignore.me:too", 1);

    HadoopConf::JobConfMap conf_map;
    conf_map[kHadoopJobUgi] = kExpectUgi;
    conf_map[kFsDefaultName] = kExpectFsName;
    HadoopConf conf("testdata/hadoop-site.xml", conf_map);
    ASSERT_EQ("testdata/hadoop-site.xml", conf.hadoop_conf_path());
    ASSERT_EQ(kExpectUgi, conf.hadoop_job_ugi());
    ASSERT_EQ(kExpectFsName, conf.fs_defaultfs());

    if (env_ugi != NULL) {
        setenv(kHadoopJobUgi, env_ugi, 1);
    } else {
        unsetenv(kHadoopJobUgi);
    }
    if (env_fs_name != NULL) {
        setenv(kFsDefaultName, env_fs_name, 1);
    } else {
        unsetenv(kFsDefaultName);
    }
}

TEST(TestHadoopConf, TestEmptyFsDefaultName) {
    char* env_fs_name = getenv(kFsDefaultName);
    unsetenv(kFsDefaultName);

    const char* kExpectUgi = "app,app";

    HadoopConf::JobConfMap conf_map;
    conf_map[kHadoopJobUgi] = kExpectUgi;
    HadoopConf conf("testdata/hadoop-site-partial.xml", conf_map);
    ASSERT_EQ("testdata/hadoop-site-partial.xml", conf.hadoop_conf_path());
    ASSERT_EQ(kExpectUgi, conf.hadoop_job_ugi());
    ASSERT_TRUE(conf.fs_defaultfs().empty());

    if (env_fs_name != NULL) {
        setenv(kFsDefaultName, env_fs_name, 1);
    }
}

} // namespace util
} // namespace flume
} // namespace baidu
