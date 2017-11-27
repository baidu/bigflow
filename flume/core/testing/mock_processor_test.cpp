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

#include "flume/core/testing/mock_processor.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/array_size.h"
#include "toft/base/scoped_ptr.h"

#include "flume/core/entity.h"
#include "flume/core/testing/mock_emitter.h"

namespace baidu {
namespace flume {

using core::Entity;
using core::Iterator;
using core::Processor;

using ::testing::_;
using ::testing::DoAll;
using ::testing::ElementsAreArray;
using ::testing::Return;
using ::testing::SetArgPointee;

TEST(MockProcessorTest, Basic) {
    const char kConfig[] = "test";
    const char* kKeys[] = {"split1", "split2"};
    Iterator* kInputs[] = {NULL, NULL};
    int object = 0;

    MockEmitter<> emitter;
    EXPECT_CALL(emitter, EmitValue(&object)).WillOnce(Return(false));
    EXPECT_CALL(emitter, Done());

    MockProcessor& mock = MockProcessor::Mock(kConfig);
    EXPECT_CALL(mock, BeginGroup(ElementsAreArray(kKeys), ElementsAreArray(kInputs)))
        .WillOnce(DoAll(EmitAndExpect(&mock, &object, false),
                        EmitDone(&mock)));
    EXPECT_CALL(mock, Process(1, &object));
    EXPECT_CALL(mock, EndGroup());


    Entity<Processor> entity = Entity<Processor>::Of<MockProcessor>(kConfig);
    toft::scoped_ptr<Processor> processor(entity.CreateAndSetup());

    processor->BeginGroup(std::vector<toft::StringPiece>(kKeys, kKeys + TOFT_ARRAY_SIZE(kKeys)),
                          std::vector<Iterator*>(kInputs, kInputs + TOFT_ARRAY_SIZE(kInputs)),
                          &emitter);
    processor->Process(1, &object);
    processor->EndGroup();
}

}  // namespace flume
}  // namespace baidu
