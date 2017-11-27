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
// Interface.

#ifndef FLUME_RUNTIME_STATUS_TABLE_H_
#define FLUME_RUNTIME_STATUS_TABLE_H_

#include <string>
#include <vector>

#include "toft/base/string/string_piece.h"

#include "flume/runtime/util/iterator.h"
#include "flume/util/arena.h"

namespace baidu {
namespace flume {
namespace runtime {

class StatusTable {
public:
    class ScopeVisitor;
    class NodeVisitor;
    typedef internal::Iterator Iterator;

    virtual ~StatusTable() {}

    virtual ScopeVisitor* GetScopeVisitor(const std::string& id,
                                          const std::vector<toft::StringPiece>& keys) = 0;

    virtual NodeVisitor* GetNodeVisitor(const std::string& id,
                                        const std::vector<toft::StringPiece>& keys) = 0;
};

class StatusTable::ScopeVisitor {
public:
    virtual ~ScopeVisitor() {}

    virtual Iterator* ListEntries() = 0;

    virtual void Release() = 0;
};

class StatusTable::NodeVisitor {
public:
    virtual ~NodeVisitor() {}

    virtual bool IsValid() = 0;

    // return true if value is loaded successfully
    virtual bool Read(std::string* value) = 0;

    virtual void Update(const std::string& value) = 0;

    virtual void Invalidate() = 0;

    virtual void Release() = 0;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_STATUS_TABLE_H_
