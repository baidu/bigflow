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
// Author: Pan Yuchang(BDG)<panyuchang@baidu.com>
// Description:

#ifndef FLUME_FLAGS_H_
#define FLUME_FLAGS_H_

#include "gflags/gflags.h"

DECLARE_int32(flume_default_concurrency);
DECLARE_int32(flume_local_max_memory_metabytes);

DECLARE_int32(storage_buffer_size_bytes);

DECLARE_int32(storage_cache_size_bytes);
DECLARE_int32(storage_leveldb_max_open_files);

#endif // FLUME_FLAGS_H_
