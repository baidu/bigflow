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

#include "flume/core/testing/mock_sinker.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/scoped_ptr.h"

#include "flume/core/entity.h"

namespace baidu {
namespace flume {

using core::Entity;
using core::Sinker;

using testing::ElementsAre;

TEST(MockSinkerTest, Basic) {
    const char kConfig[] = "test";
    int object = 0;

    MockSinker& mock = MockSinker::Mock(kConfig);
    EXPECT_CALL(mock, Open(ElementsAre("global")));
    EXPECT_CALL(mock, Sink(&object));
    EXPECT_CALL(mock, Close());

    Entity<Sinker> entity = Entity<Sinker>::Of<MockSinker>(kConfig);
    toft::scoped_ptr<Sinker> sinker(entity.CreateAndSetup());
    sinker->Open(std::vector<toft::StringPiece>(1, "global"));
    sinker->Sink(&object);
    sinker->Close();
}

}  // namespace flume
}  // namespace baidu
