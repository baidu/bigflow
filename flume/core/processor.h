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
// Interface, for execution node which takes one input flow and one output flow. Corresponding
// to PROCESS_NODE in logical execution plan. See flume/doc/core.rst for details.

#ifndef FLUME_CORE_PROCESSOR_H_
#define FLUME_CORE_PROCESSOR_H_

#include <stdint.h>
#include <map>
#include <string>
#include <vector>

#include "toft/base/string/string_piece.h"

#include "flume/core/status.h"
#include "flume/core/emitter.h"
#include "flume/core/iterator.h"

namespace baidu {
namespace flume {
namespace core {

// BeginGroup and Process can return false to cancel execuation false to cancel
// execuation.
class Processor : public IStatus {
public:
    virtual ~Processor() {}

    virtual void Setup(const std::string& config) = 0;

    // TODO(wenxiang): add '= 0' after all users implements this interface
    virtual std::string Synchronize() { return std::string(); }

    // Begin processing a group of records with the same key
    // @param keys: Contains keys generated in all covering scope.
    // @param inputs: Input[i] will be NULL if input-i is not prepared
    // @param emitter: Used to commit result. calling emitter->Done() can cancel
    //                 processing, further Process will NOT be called. However,
    //                 EndGroup are guaranteed to be called.
    virtual void BeginGroup(const std::vector<toft::StringPiece>& keys,
                            const std::vector<Iterator*>& inputs,
                            Emitter* emitter) = 0;

    virtual void Process(uint32_t index, void* object) = 0;

    virtual void EndGroup() = 0;

    // save loader status to buffer if has enough buffer
    // return serialize bytes size
    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size) { return 0; }

    // return an address to deserialized loader.
    virtual bool Deserialize(const char* buffer, uint32_t buffer_size) { return true; }

    virtual uint32_t ObjectSize() { return sizeof(Processor); }
};

}  // namespace core
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_CORE_PROCESSOR_H_
