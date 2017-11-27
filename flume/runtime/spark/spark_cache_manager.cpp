/***************************************************************************
 *
 * Copyright (c) 2016 Baidu, Inc. All Rights Reserved.
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
// Author: Wang Cong (bigflow-opensource@baidu.com)

#include <map>
#include <string>
#include <vector>
#include <flume/runtime/common/cache_manager.h>

#include "boost/make_shared.hpp"
#include "boost/algorithm/hex.hpp"

#include "toft/crypto/uuid/uuid.h"
#include "toft/storage/file/file.h"
#include "toft/storage/seqfile/local_sequence_file_reader.h"
#include "toft/storage/seqfile/local_sequence_file_writer.h"

#include "flume/runtime/backend.h"
#include "flume/runtime/spark/spark_cache_manager.h"
#include "flume/runtime/spark/shuffle_protocol.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

const size_t kCacheKeyBufferSize = 4 * 1024;

ShuffleHeader* NewBuffer(size_t size, uint32_t task_id) {
    CHECK_GE(size, sizeof(ShuffleHeader));
    char* ret = new char[size];
    ShuffleHeader* header = ShuffleHeader::cast(ret);
    header->set(task_id, 0, false);
    return header;
}

class SparkCacheManager::Writer : public flume::runtime::CacheManager::Writer {
public:
    Writer(const Emitter& emitter, uint32_t task_id) : m_emitter(emitter),
                                                       m_key_buf(kCacheKeyBufferSize, '\0'),
                                                       m_task_id(task_id) {
    }

    virtual bool BeginKeys(const std::vector<toft::StringPiece>& keys) {
        CHECK_NE(0u, keys.size());
        uint32_t total_size = sizeof(ShuffleHeader);
        for (size_t i = 0; i != keys.size(); ++i) {
            total_size += keys[i].size() + sizeof(uint32_t);
        }
        total_size -= sizeof(uint32_t);

        if (total_size > m_key_buf.capacity()) {
            m_key_buf.resize(std::max<uint32_t>(total_size, m_key_buf.capacity() * 2));
        }
        char* buf = &(*m_key_buf.begin());
        m_key.set(buf, total_size);

        ShuffleHeader* header = ShuffleHeader::cast(buf);
        header->set(m_task_id, 0, false);
        char* content = const_cast<char*>(header->content());
        for (size_t i = 0; i != keys.size() - 1; ++i) {
            uint32_t size = keys[i].size();
            uint32_t size_to_cp = htonl(size);
            memcpy(content, &size_to_cp, sizeof(size));
            memcpy(content + sizeof(size), keys[i].data(), size);
            content += size + sizeof(size);
        }
        memcpy(content, keys.back().data(), keys.back().size());
        return true;
    }

    virtual bool Write(const toft::StringPiece& value) {
        m_emitter(m_key, value);
        return true;
    }

    virtual bool EndKeys() { return true; }

    bool Close() {
        return true;
    }

    ~Writer() {
    }

private:
    Emitter m_emitter;
    std::string m_key_buf;
    toft::StringPiece m_key;
    uint32_t m_task_id;
};

CacheManager::Reader* SparkCacheManager::GetReader(const std::string& node_id) {
    return NULL;
}

CacheManager::Writer* SparkCacheManager::GetWriter(const std::string& node_id) {
    typedef SparkCacheManager::Writer Writer;
    BOOST_AUTO(iter, m_writers.find(node_id));
    if (iter == m_writers.end()) {
        CHECK_EQ(1u, m_cache_node_to_task_id.count(node_id));
        auto writer = boost::make_shared<Writer>(m_emitter, m_cache_node_to_task_id[node_id]);
        auto pair = std::make_pair(node_id, writer);
        auto inserted = m_writers.insert(pair);
        CHECK(inserted.second);
        return inserted.first->second.get();
    }
    return iter->second.get();
}

void SparkCacheManager::Shutdown() {
    //m_readers.clear();
    m_writers.clear();
}

SparkCacheManager::SparkCacheManager(const Emitter& emitter,
                                     const PbSparkJob::PbSparkJobInfo& job_info)
        : m_emitter(emitter) {
    CHECK_EQ(job_info.cache_node_id_size(), job_info.cache_task_id_size());
    for (size_t i = 0; i != job_info.cache_node_id_size(); ++i) {
        m_cache_node_to_task_id[job_info.cache_node_id(i)] = job_info.cache_task_id(i);
    }

    LOG(INFO) << "SparkCacheManager Constructed";
}


}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
