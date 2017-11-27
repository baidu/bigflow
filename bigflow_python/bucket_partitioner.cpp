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
// Author: Xu Yao <xuyao02@baidu.com>
//
// A class for partitioner.

#include "bigflow_python/bucket_partitioner.h"
#include "bigflow_python/proto/entity_config.pb.h"

#include "glog/logging.h"
#include "toft/system/time/timestamp.h"

namespace baidu {
namespace bigflow {

void BucketPartitioner::Setup(const std::string& config) {
    _first_round = true;
    _sequence = 0;
    PbBucketPartitionerConfig pb_config;
    CHECK(pb_config.ParseFromString(config));
    _bucket_size = pb_config.has_bucket_size() ? pb_config.bucket_size() : 1;
    CHECK_LE(static_cast<uint64_t>(1), _bucket_size);
}

uint32_t BucketPartitioner::Partition(void* object, uint32_t partition_number) {
    if (_first_round) {
        unsigned seed = toft::GetTimestampInMs() + getpid();
        _key = static_cast<uint32_t>(rand_r(&seed) % partition_number);
        _first_round = false;
    }

    if (_sequence++ == _bucket_size - 1) {
        _key += 1;
        _sequence = 0;
    }
    return _key % partition_number;
}

BucketPartitioner::BucketPartitioner() {}

BucketPartitioner::~BucketPartitioner() {}

}  // namespace bigflow
}  // namespace baidu

