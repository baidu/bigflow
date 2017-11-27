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

#include "flume/planner/common/cache_util.h"
#include "flume/runtime/io/io_format.h"
#include "boost/lexical_cast.hpp"
namespace baidu {
namespace flume {
namespace planner {

namespace {

uint32_t EncodeString(char *buf, uint32_t offset, uint32_t size,
                  const toft::StringPiece &str) {
    int32_t str_size = str.size();
    memcpy(buf + offset, &str_size, sizeof(str_size));
    offset += sizeof(str_size);
    memcpy(buf + offset, str.data(), str.size());
    offset += str_size;

    return offset;
}

} // anoymous namespace

uint32_t CacheRecordObjector::Serialize(void *object, char *buf, uint32_t buf_size) {
    CacheRecord *cache_record = reinterpret_cast<CacheRecord*>(object);
    typedef std::vector<std::string> VC;
    const VC &keys = cache_record->keys;

    // weird...
    uint32_t need = 0;
    for (VC::const_iterator it = keys.begin(); it != keys.end(); ++it) {
        need += it->size() + sizeof(int32_t);
    }
    need += sizeof(int32_t);
    need += cache_record->content.size();
    need += sizeof(bool);
    if (need > buf_size) {
        return need;
    }

    uint32_t offset = 0;
    for (VC::const_iterator it = keys.begin(); it != keys.end(); ++it) {
        offset = EncodeString(buf, offset, buf_size, *it);
    }
    offset = EncodeString(buf, offset, buf_size, cache_record->content);

    // encoding empty
    memcpy(buf + offset, &(cache_record->empty), sizeof(bool));
    offset += sizeof(bool);
    return offset;
}

void* CacheRecordObjector::Deserialize(const char *buf, uint32_t buf_size) {
    CacheRecord *cache_record = new CacheRecord();
    uint32_t used = 0;
    int32_t size = 0;
    // problem prone
    while (used < buf_size - sizeof(bool)) {
        memcpy(&size, buf + used, sizeof (size));
        used += sizeof (size);
        cache_record->keys.push_back(std::string(buf+used, size));
        used += size;
    }
    std::vector<std::string> &keys = cache_record->keys;
    CHECK(keys.size() > 0);
    cache_record->content.set(buf + buf_size - size - sizeof(bool), size);
    keys.resize(keys.size() - 1);

    cache_record->empty = buf[buf_size - sizeof(bool)];
    return cache_record;
}

void CacheRecordObjector::Release(void *object) {
    CacheRecord *cache_record = reinterpret_cast<CacheRecord*>(object);
    delete cache_record;
}

void LevelKeyReader::Setup(const std::string &config) { m_level = boost::lexical_cast<uint32_t>(config); }

uint32_t LevelKeyReader::ReadKey(void *object, char *out_buf, uint32_t out_buf_size) {
    CacheRecord *record = reinterpret_cast<CacheRecord*>(object);
    // level-0 is key from global scope
    // for storage efficency, we omit this key
    CHECK_GT(m_level, 0);
    CHECK_LT(m_level, record->keys.size());
    const std::string &key = record->keys[m_level];
    if (key.size() <= out_buf_size) {
        memcpy(out_buf, key.data(), key.size());
    }
    return key.size();
}


void LevelPartitioner::Setup(const std::string &config) { m_level = boost::lexical_cast<uint32_t>(config); }
uint32_t LevelPartitioner::Partition(void *object, uint32_t partitioner_number) {
        // mute compiler warnning
        (void)(partitioner_number);

        CacheRecord *record = reinterpret_cast<CacheRecord*>(object);
        // level-0 is key from global scope
        // for storage efficency, we omit this key
        CHECK(m_level > 0);
        CHECK(m_level < record->keys.size());
        const std::string &key = record->keys[m_level];
        return boost::lexical_cast<uint32_t>(key);
    }


//
void CacheReader::Process(uint32_t index, void *object) {
    using baidu::flume::runtime::Record;
    CHECK(index == 0);
    Record *record = reinterpret_cast<Record*>(object);
    // fixme:
    // code below is not safe given possibility of data corrupt exist
    if (record->key == "s") {
        // we need to emit it.
        if (m_cache_record.empty == true) {
            std::string str = "";
            m_cache_record.content = str;
            m_emitter->Emit(&m_cache_record);
        }
        m_cache_record.keys.clear();
        m_cache_record.keys.push_back(std::string(record->value.data(), record->value.size()));
    } else if (record->key == "k") {
        m_cache_record.keys.push_back(std::string(record->value.data(), record->value.size()));
        m_cache_record.empty = true;
    } else {
        m_cache_record.content = record->value;
        m_cache_record.empty = false;
        m_emitter->Emit(&m_cache_record);
    }
}

void StripKeyProcessor::Process(uint32_t index, void *object) {
    CHECK(index == 0);
    CacheRecord *cache_record = reinterpret_cast<CacheRecord*>(object);
    if (cache_record->empty == true) {
        return;
    }
    toft::StringPiece content = cache_record->content;
    void* output = m_objector->Deserialize(content.data(), content.size());
    m_emitter->Emit(output);
    m_objector->Release(output);
}

PbEntity LevelKeyReaderEntity(uint32_t level) {
    return Entity<KeyReader>::Of<LevelKeyReader>
        (boost::lexical_cast<std::string>(level)).ToProtoMessage();
}

PbEntity LevelPartitionerEntity(uint32_t level) {
    return Entity<Partitioner>::Of<LevelPartitioner>
        (boost::lexical_cast<std::string>(level)).ToProtoMessage();
}

} // namespace planner
} // namespace flume
} // namespace baidu
