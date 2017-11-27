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
// Author: miaodongdong (bigflow-opensource@baidu.com)
//
#ifndef BIGFLOW_FLUME_RUNTIME_SPARK_TASK_ENV_H_
#define BIGFLOW_FLUME_RUNTIME_SPARK_TASK_ENV_H_

/*
 * This class provides a way to add an intial-callback before every task running.
 *
 * FIXME(miaodongdong):
 *    It's not a elegent way to set a callback with the static class member.
 *    We should let flume on spark support flume::core::Environment, and use it to replace this one in the future.
 *
 * */

class FlumeSparkTaskEnv {
public:
    typedef void (*FlumeSparkTaskCallback)();
    static FlumeSparkTaskCallback setup_callback;
};

#endif //BIGFLOW_FLUME_RUNTIME_SPARK_TASK_ENV_H_
