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
// Author: Wen Xiang <wenxiang@baidu.com>

#ifndef FLUME_CORE_TESTING_MOCK_PARTITIONER_H_
#define FLUME_CORE_TESTING_MOCK_PARTITIONER_H_

#include <cstring>
#include <string>

#include "boost/ptr_container/ptr_map.hpp"
#include "gmock/gmock.h"

#include "flume/core/partitioner.h"
#include "flume/core/testing/mock_base.h"

namespace baidu {
namespace flume {

class MockPartitioner : public MockBase< ::baidu::flume::core::Partitioner, MockPartitioner> {
public:
    class PartitionSetter {
    public:
        PartitionSetter(MockPartitioner* base, void* object, uint32_t partition_number)
                : m_base(base), m_object(object), m_partition_number(partition_number) {}

        void operator=(uint32_t partition) const {
            using ::testing::Return;

            EXPECT_CALL(*m_base, Partition(m_object, m_partition_number))
                .WillRepeatedly(Return(partition));
        }

    private:
        MockPartitioner* m_base;
        void* m_object;
        uint32_t m_partition_number;
    };

public:
    MOCK_METHOD2(Partition, uint32_t (void*, uint32_t));  // NOLINT

    void Delegate(MockPartitioner* mock) {
        using ::testing::_;
        using ::testing::AnyNumber;
        using ::testing::Invoke;

        ON_CALL(*this, Partition(_, _))
            .WillByDefault(Invoke(mock, &MockPartitioner::Partition));
        EXPECT_CALL(*this, Partition(_, _)).Times(AnyNumber());
    }

    PartitionSetter PartitionOf(void* object, uint32_t partition_number) {
        return PartitionSetter(this, object, partition_number);
    }
};

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_MOCK_PARTITIONER_H_
