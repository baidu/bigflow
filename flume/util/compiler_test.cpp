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
// Author: Liang Kun <bigflow-opensource@baidu.com>

#include "flume/util/compiler.h"
#include <cstdarg>
#include <cmath>
#include "gtest/gtest.h"

using std::abs;

TEST(CompilerTest, Likelity) {
    int result;
    const int if_result = 10;
    const int else_result = 20;

    if (LIKELY(abs(-10) > 0)) {
        result = if_result;
    } else {
        result = else_result;
    }
    EXPECT_EQ(if_result, result);

    if (LIKELY(abs(-10) < 0)) {
        result = if_result;
    } else {
        result = else_result;
    }
    EXPECT_EQ(else_result, result);

    if (UNLIKELY(abs(-10) > 0)) {
        result = if_result;
    } else {
        result = else_result;
    }
    EXPECT_EQ(if_result, result);

    if (UNLIKELY(abs(-10) < 0)) {
        result = if_result;
    } else {
        result = else_result;
    }
    EXPECT_EQ(else_result, result);
}

TEST(CompilerTest, Prefetch) {
    const int kNumElems = 10;
    int elements[kNumElems] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
    int sum_normal = 0;
    int sum_prefetched = 0;

    for (int i = 0; i < kNumElems; ++i) {
        sum_normal += elements[i];
    }

    PREFETCH(&elements[0]);
    for (int i = 0; i < kNumElems; ++i) {
        PREFETCH(&elements[i+1]);
        sum_prefetched += elements[i];
    }

    EXPECT_EQ(sum_normal, sum_prefetched);
}

TEST(CompilerTest, VaNumArgs) {
    EXPECT_EQ(0, VA_NUM_ARGS());
    EXPECT_EQ(1, VA_NUM_ARGS(1));
    EXPECT_EQ(2, VA_NUM_ARGS(1, 2));
    EXPECT_EQ(5, VA_NUM_ARGS(1, 2, 3, 4, 5));
    EXPECT_EQ(9, VA_NUM_ARGS(1, 2, 3, 4, 5, 6, 7, 8, 9));
}
