/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
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

// Author: daiweiwei01@baidu.com
//
#ifndef FLUME_PLANNER_CACHE_CACHE_UTIL_H_
#define FLUME_PLANNER_CACHE_CACHE_UTIL_H_

#include <cstdlib>
#include <string>
#include "flume/core/entity.h"
#include "flume/core/key_reader.h"
#include "flume/core/partitioner.h"
#include "flume/core/processor.h"
#include "flume/core/objector.h"
#include "flume/core/loader.h"
#include "flume/runtime/io/io_format.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "flume/proto/entity.pb.h"

namespace baidu {
namespace flume {
namespace planner {

struct CacheRecord {
    std::vector<std::string> keys;
    toft::StringPiece content;
    bool empty;
};

class CacheRecordObjector : public core::Objector {
public:
    virtual ~CacheRecordObjector () {}

    virtual void Setup(const std::string& config) { }

    virtual uint32_t Serialize(void* object, char* buffer, uint32_t buffer_size);

    virtual void* Deserialize(const char* buffer, uint32_t buffer_size);

    virtual void Release(void* object);
};

// key reader and partitioner for KV loaded from cache
class LevelKeyReader : public core::KeyReader {
public:
    virtual ~LevelKeyReader() { }
    virtual void Setup(const std::string &config);
    virtual uint32_t ReadKey(void *object, char *out_buf, uint32_t out_buf_size);

private:
    uint32_t m_level;
};

class LevelPartitioner : public core::Partitioner {
public:
    virtual ~LevelPartitioner() { }
    virtual void Setup(const std::string &config);
    virtual uint32_t Partition(void *object, uint32_t partitioner_number);

private:
    uint32_t m_level;
};

class CacheReader : public core::Processor {
public:
    virtual ~CacheReader() { }
    virtual void Setup(const std::string& config) { }

    virtual void BeginGroup(const std::vector<toft::StringPiece>& keys,
                            const std::vector<core::Iterator*>& inputs,
                            core::Emitter* emitter) {
        m_emitter = emitter;
        m_cache_record.empty = false;
    }

    virtual void Process(uint32_t index, void* object);

    virtual void EndGroup() {
        if (m_cache_record.empty == true) {
            std::string str = "";
            m_cache_record.content = str;
            m_emitter->Emit(&m_cache_record);
        }
    }
private:
    core::Emitter *m_emitter;
    CacheRecord m_cache_record;
};

class StripKeyProcessor : public core::Processor {
public:
    virtual ~StripKeyProcessor() { }
    virtual void Setup(const std::string& config) {
        PbEntity pb_entity;
        CHECK(pb_entity.ParseFromString(config));
        core::Entity<core::Objector> entity =
            core::Entity<core::Objector>::From(pb_entity);
        m_objector.reset(entity.CreateAndSetup());
    }
    virtual void BeginGroup(const std::vector<toft::StringPiece>& keys,
                            const std::vector<core::Iterator*>& inputs,
                            core::Emitter* emitter) { m_emitter = emitter; }

    virtual void Process(uint32_t index, void* object);

    virtual void EndGroup() { }
private:
    core::Emitter *m_emitter;
    toft::scoped_ptr<core::Objector> m_objector;
};

// TO be removed
using namespace core;
// @param "config" is the level hint
inline PbEntity GetLevelKeyReader(const std::string &config) {
    // NRVO
    return Entity<KeyReader>::Of<LevelKeyReader>(config).ToProtoMessage();
}

inline PbEntity GetLevelPartitioner(const std::string &config) {
    return Entity<Partitioner>::Of<LevelPartitioner>(config).ToProtoMessage();
}

inline PbEntity CacheLoaderPbEntity(void) {
    return Entity<Loader>::Of<runtime::SequenceFileAsBinaryInputFormat>("").ToProtoMessage();
}

inline PbEntity CacheReaderPbEntity(void) {
    return Entity<Processor>::Of<CacheReader>("").ToProtoMessage();
}

inline PbEntity StripKeyProcessorEntity(const std::string &config) {
    return Entity<Processor>::Of<StripKeyProcessor>(config).ToProtoMessage();
}

inline PbEntity CacheRecordObjectorEntity(void) {
    return Entity<Objector>::Of<CacheRecordObjector>("").ToProtoMessage();
}

PbEntity LevelKeyReaderEntity(uint32_t level);

PbEntity LevelPartitionerEntity(uint32_t level);

} // namespace baidu
} // namespace flume
} // namespace planner

#endif
