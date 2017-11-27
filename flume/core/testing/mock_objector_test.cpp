/************************************************************************
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
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#include "flume/core/testing/mock_objector.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"

#include "flume/core/entity.h"

namespace baidu {
namespace flume {

using core::Entity;
using core::Objector;

using ::testing::Return;

TEST(MockObjectTest, Basic) {
    const char kConfig[] = "test";

    char buffer[16];
    size_t buffer_size = sizeof(buffer);
    int object = 0;

    MockObjector& mock = MockObjector::Mock(kConfig);
    EXPECT_CALL(mock, Serialize(&object, buffer, buffer_size))
        .WillOnce(Return(sizeof(object)));
    EXPECT_CALL(mock, Deserialize(buffer, sizeof(object)))
        .WillOnce(Return(&object));
    EXPECT_CALL(mock, Release(&object));

    Entity<Objector> entity = Entity<Objector>::Of<MockObjector>(kConfig);
    toft::scoped_ptr<Objector> objector(entity.CreateAndSetup());
    ASSERT_EQ(sizeof(object), objector->Serialize(&object, buffer, buffer_size));
    ASSERT_EQ(static_cast<void*>(&object), objector->Deserialize(buffer, sizeof(object)));
    objector->Release(&object);
}

}  // namespace flume
}  // namespace baidu
