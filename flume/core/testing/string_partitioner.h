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

#ifndef FLUME_CORE_TESTING_STRING_PARTITIONER_H_
#define FLUME_CORE_TESTING_STRING_PARTITIONER_H_

#include <cstring>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "flume/core/partitioner.h"
#include "flume/core/testing/mock_base.h"

namespace baidu {
namespace flume {

class StringPartitioner
        : public MockBase< ::baidu::flume::core::Partitioner, StringPartitioner> {
public:
    class PartitionSetter {
    public:
        PartitionSetter(StringPartitioner* base,
                        const std::string& object, uint32_t partition_number)
                : m_base(base), m_object(object), m_partition_number(partition_number) {}

        void operator=(uint32_t partition) const {
            using ::testing::Return;

            EXPECT_CALL(*m_base, RealPartition(m_object, m_partition_number))
                .WillRepeatedly(Return(partition));
        }

    private:
        StringPartitioner* m_base;
        std::string m_object;
        uint32_t m_partition_number;
    };

public:
    MOCK_METHOD2(RealPartition, uint32_t (const std::string&, uint32_t));  // NOLINT

    void Delegate(StringPartitioner* mock) {
        using ::testing::_;
        using ::testing::AnyNumber;
        using ::testing::Invoke;

        ON_CALL(*this, RealPartition(_, _))
            .WillByDefault(Invoke(mock, &StringPartitioner::RealPartition));
        EXPECT_CALL(*this, RealPartition(_, _)).Times(AnyNumber());
    }

    PartitionSetter PartitionOf(const std::string& object, uint32_t partition_number) {
        return PartitionSetter(this, object, partition_number);
    }

private:
    virtual uint32_t Partition(void* object, uint32_t partition_number) {
        return RealPartition(*static_cast<std::string*>(object), partition_number);
    }
};

}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_TESTING_STRING_PARTITIONER_H_
