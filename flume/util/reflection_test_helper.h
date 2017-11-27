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
// Helper for testing Reflection.

#ifndef FLUME_UTIL_REFLECTION_TEST_HELPER_H_
#define FLUME_UTIL_REFLECTION_TEST_HELPER_H_

#include <string>

// base types for tests

class Fn {
public:
    virtual std::string ToString() {
        return "fn";
    }

    virtual ~Fn() {}
};

template<typename T>
class TFn {
public:
    virtual T Value() = 0;

    virtual ~TFn() {}
};

std::string GetUnNamedType();

#endif  // FLUME_UTIL_REFLECTION_TEST_HELPER_H_
