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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//
// A testing helper class to mark a executioin point. Used with gmock.

#ifndef FLUME_RUNTIME_TESTING_TEST_MARKER_H_
#define FLUME_RUNTIME_TESTING_TEST_MARKER_H_

#include <string>

#include "gmock/gmock.h"

namespace baidu {
namespace flume {
namespace runtime {

class TestMarker {
public:
    // used to mark an expectation
    MOCK_METHOD1(Mark, void (const std::string&));  // NOLINT
    MOCK_METHOD1(mark, void (const std::string&));  // NOLINT
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_TESTING_TEST_MARKER_H_
