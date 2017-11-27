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
//
// A list of helper motheds to create fake pointer. reinterpret_cast<...> is too long to
// type.

#ifndef FLUME_RUNTIME_TESTING_FAKE_POINTER_H_
#define FLUME_RUNTIME_TESTING_FAKE_POINTER_H_

#include "flume/runtime/dispatcher.h"

namespace baidu {
namespace flume {
namespace runtime {

void* ObjectPtr(int x) {
    return reinterpret_cast<void*>(x);
}

core::Iterator* IteratorPtr(int x) {
    return reinterpret_cast<core::Iterator*>(x);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_TESTING_FAKE_POINTER_H_
