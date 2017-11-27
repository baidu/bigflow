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
// Author: Liang Kun <liangkun@baidu.com>
//
// This file defines compiler-dependent macro utilities.
// These macros are known to work under gcc-3.4.5 and clang-3.3.

#ifndef FLUME_UTIL_COMPILER_H_
#define FLUME_UTIL_COMPILER_H_

#ifdef __GNUC__
#define FLUME_GCC_VERSION (__GNUC__ * 10000 \
    + __GNUC_MINOR__ * 100 \
    + __GNUC_PATCHLEVEL__)
#else
#define FLUME_GCC_VERSION 0
#endif

// Compiler hint that this branch is likely or unlikely to
// be taken. Take from the "What all programmers should know
// about memory" paper.
// Examples:
//     if (LIKELY(size > 0)) { ... }
//     if (UNLIKELY(!status.ok())) { ... }
#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)

#define PREFETCH(addr) __builtin_prefetch(addr)

// Counts the number of arguments in a __VA_ARGS__ list.
// Currently supports from 0 up to 9 arguments.
// Examples:
// int num = VA_NUM_ARGS(x, y, z);  // num evaluates to 3
//
// double average(int count, ...) {
//     double sum = 0.0;
//     va_list args;
//     va_start(args, count);
//     for (int i = 0; i < count; ++i) {
//         sum += va_arg(args, double);
//     }
//     va_end(args);
//     return sum / count;
// }
//
// #define AVG(...) average(VA_NUM_ARGS(__VA_ARGS__), ##__VA_ARGS__);
//
// double avg = AVG(3.14, 2.71, 1024.1023);
//
#define VA_NUM_ARGS(...) \
    VA_NUM_ARGS_IMPL__(0, ##__VA_ARGS__, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#define VA_NUM_ARGS_IMPL__(_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, N, ...) N

#endif  // FLUME_UTIL_COMPILER_H_
