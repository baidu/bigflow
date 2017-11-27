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
// Author: Xu Yao <bigflow-opensource@baidu.com>
//
// A class for partitioner.

#ifndef BLADE_BIGFLOW_PARTITIONERS_BUCKET_PARTITIONER_H
#define BLADE_BIGFLOW_PARTITIONERS_BUCKET_PARTITIONER_H

#include "flume/core/partitioner.h"

namespace baidu {
namespace bigflow {

class BucketPartitioner : public flume::core::Partitioner {
public:
    BucketPartitioner();
    virtual ~BucketPartitioner();

    virtual void Setup(const std::string& config);

    virtual uint32_t Partition(void* object, uint32_t partition_number);

private:
    bool _first_round;
    uint32_t _key;
    uint64_t _bucket_size;
    uint64_t _sequence;
};

}  // namespace bigflow
}  // namespace baidu

#endif  // BLADE_BIGFLOW_PARTITIONERS_BUCKET_PARTITIONER_H
