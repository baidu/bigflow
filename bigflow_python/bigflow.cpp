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
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#include "bigflow_python/bigflow.h"

#include "gflags/gflags.h"

DECLARE_bool(flume_dce_enable_shrink);
DECLARE_bool(flume_dce_v2);
DECLARE_bool(flume_log_service);
DECLARE_int32(flume_default_concurrency);

DECLARE_bool(flume_execute);

namespace baidu {
namespace bigflow {

void InitBigflow(int argc, char** argv, InitialProcessCallback callback) {
    // Set some default flags

    // Enable shrink again when CacheWriter is refined.
    //FLAGS_flume_dce_enable_shrink = true;

    // Enable dce_v2 by default. Remove this flag when v1 is removed.
    //FLAGS_flume_dce_v2 = true;
    FLAGS_flume_default_concurrency = 1000;
    ::google::ParseCommandLineFlags(&argc, &argv, true/*remove flags*/);

    if (FLAGS_flume_execute && callback) {
        callback();
    }

    ::baidu::flume::InitBaiduFlume();
}

}  // namespace bigflow
}  // namespace baidu

