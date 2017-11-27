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
// Author: Wang Cong<wangcong09@baidu.com>

#include "gflags/gflags.h"
#include "flume/runtime/spark/spark_task_env.h"

FlumeSparkTaskEnv::FlumeSparkTaskCallback FlumeSparkTaskEnv::setup_callback = NULL;

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

DEFINE_string(flume_python_home, "", "path of CPython at runtime");
DEFINE_string(flume_application_home, "", "base ath of bigflow on spark application running on yarn");

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
