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
// Interface. Similar to spark's RDD (Resilient Distributed Dataset), however, our Dataset is
// only a dataset, which means it does not have ability to re-generate its data.

#ifndef FLUME_RUNTIME_DATASET_H_
#define FLUME_RUNTIME_DATASET_H_

#include <string>

#include "toft/base/string/string_piece.h"

#include "flume/runtime/util/iterator.h"
#include "flume/util/arena.h"

namespace baidu {
namespace flume {
namespace runtime {

// Dataset represents a group of records, which is part of outputs by LogicalPlanNode (see
// flume/doc/core.rst).
// Datasets are organized according to the concept of Scope.
class Dataset {
public:
    typedef internal::Iterator Iterator;

    virtual ~Dataset() {}

    // IsReady means this Dataset is readable and immutable.
    virtual bool IsReady() = 0;

    // Get an iterator to access datas. Must be called when IsReady() returns true.
    virtual Iterator* NewIterator() = 0;

    // All datas for this dataset is completed, and iterators can be created from this Dataset.
    // There should be no more changes after calling Commit.
    virtual void Commit() = 0;

    // Contents of this dataset is not needed any more. Return an iterator to access
    // existing datas.
    // Note that Discard only means iterators of this dataset is not needed, we can still
    // create new childs.
    virtual Iterator* Discard() = 0;

    // If the dataset for the given key is already committed, return it. Else create a new one.
    // Not legal for lowest level datasets.
    virtual Dataset* GetChild(const toft::StringPiece& key) = 0;

    // Emit data. Note that buffer for the emitted data should be stable, or allocated
    // from Arena().
    virtual void Emit(const toft::StringPiece& value) = 0;
    virtual util::Arena* AcquireArena() = 0;
    virtual void ReleaseArena() = 0;

    // this Dateset is not needed any more.
    virtual void Release() = 0;

    virtual void AddReference() = 0;
};

// behave as a cache manager.
class DatasetManager {
public:
    virtual ~DatasetManager() {}

    virtual Dataset* GetDataset(const std::string& identity) = 0;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_DATASET_H_
