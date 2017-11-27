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
// This file gives inferface to communicate between execution nodes. Execution node can be
// any instance of Loader/Sinker/Processor. Details of those execution nodes can be
// seen in flume/doc/core.rst.

#ifndef FLUME_CORE_EMITTER_H_
#define FLUME_CORE_EMITTER_H_

namespace baidu {
namespace flume {
namespace core {

// Emitter are used to pass result to subsequent execution node.
class Emitter {
public:
    virtual ~Emitter() {}

    // Emit result to subsequent execution node. When Emit returns, object is not needed any more.
    // If Emit returns false, means no more records is needed by subsequence nodes.
    virtual bool Emit(void *object) = 0;

    // No more outputs. User can explicit calling it to cancel execution.
    virtual void Done() = 0;
};

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_EMITTER_H_
