/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
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
// Author: Zhang Yuncong (bigflow-opensource@baidu.com)

#include <map>
#include <string>
#include <vector>

#include "boost/make_shared.hpp"

#include "toft/crypto/uuid/uuid.h"
#include "toft/storage/file/file.h"
#include "toft/storage/seqfile/local_sequence_file_reader.h"
#include "toft/storage/seqfile/local_sequence_file_writer.h"

#include "flume/runtime/backend.h"
#include "flume/runtime/common/file_cache_manager.h"
#include "flume/util/path_util.h"

namespace baidu {
namespace flume {
namespace runtime {

class FileCacheManager::Iterator : public CacheManager::Iterator {
public:

    Iterator(const std::string& split) {
        m_file_name = split;
        Reset();
    }

    virtual bool Next() {
        CHECK(m_reader.get() != NULL) << "read an empty reader, do you have already called Done ?";
        toft::StringPiece key;
        toft::StringPiece value;
        while (m_reader->ReadRecord(&key, &value)) {
            if (key == "s") { // means start of the key
                m_keys.clear();
                m_keys_holder.clear();
                m_keys_holder.push_back(value.as_string());
                m_keys.push_back(toft::StringPiece(m_keys_holder.back()));
            } else if (key == "k") { // means the continue keys
                m_keys_holder.push_back(value.as_string());
                m_keys.push_back(toft::StringPiece(m_keys_holder.back()));
            } else {
                m_value = value;
                return true;
            }
        }
        CHECK(m_reader->is_finished());
        m_reader.reset();
        return false;
    }

    virtual void Reset() {
        toft::scoped_ptr<toft::File> file(toft::File::Open(m_file_name, "r"));
        CHECK(file.get() != NULL) << "File open error : " << m_file_name;
        m_reader.reset(new toft::LocalSequenceFileReader(file.release()));
        CHECK(m_reader.get() != NULL) << "Seq File open error : " << m_file_name;

    }

    virtual void Done() {
        m_reader.reset();
    }

    virtual const std::vector<toft::StringPiece>& Keys() {
        return m_keys;
    }

    virtual const toft::StringPiece& Value() {
        return m_value;
    }

private:
    std::string m_file_name;
    toft::scoped_ptr<toft::LocalSequenceFileReader> m_reader;
    std::vector<toft::StringPiece> m_keys;
    std::vector<std::string> m_keys_holder;
    toft::StringPiece m_value;

    bool m_has_next;
};

class FileCacheManager::Reader : public CacheManager::Reader {
public:
    Reader(const std::string& uri) : m_uri(uri) {}

    virtual void GetSplits(std::vector<std::string>* splits) const {
        using toft::FileIterator;
        LOG(INFO) <<"split uri : "
            << flume::util::HideToftPathPassword(m_uri);
        FileIterator* iter
            = toft::File::Iterate(m_uri, "*", toft::FileType_Regular | toft::FileType_Link);
        toft::scoped_ptr<FileIterator> iterator(iter);
        CHECK(NULL != iterator) << "List file [" << m_uri << "/*] failed";
        toft::FileEntry entry;
        while(iterator->GetNext(&entry)) {
            LOG(INFO) << "get file " << entry.name;
            splits->push_back(m_uri + "/" + entry.name);
        }
    }

    virtual FileCacheManager::Iterator* Read(const std::string& split) {
        m_iterator.reset(new FileCacheManager::Iterator(split));
        return m_iterator.get();
    }

private:
    std::string m_uri;
    toft::scoped_ptr<FileCacheManager::Iterator> m_iterator;
};

class FileCacheManager::Writer : public CacheManager::Writer {
public:
    Writer(const std::string& uri, const std::string& task_id) : m_uri(uri), m_task_id(task_id) {
        CHECK(Open());
    }

    bool Open() {
        m_target_uri = m_uri + "/";
        m_tmp_uri = m_uri + "/_tmp/";
        if (!util::IsHdfsPath(m_target_uri)) {
            std::string cmd = "mkdir -p ";
            cmd += m_target_uri;
            CHECK_EQ(0, system(cmd.c_str()));
            cmd = "mkdir -p ";
            cmd += m_tmp_uri;
            CHECK_EQ(0, system(cmd.c_str()));
        }
        m_target_uri += m_task_id;
        m_tmp_uri += m_task_id + "." + toft::CreateCanonicalUUIDString();
        toft::scoped_ptr<toft::File> file(toft::File::Open(m_tmp_uri, "w"));
        m_writer.reset(new toft::LocalSequenceFileWriter(file.release()));
        if (!m_writer->Init()) {
            return false;
        }
        m_closed = false;
        return true;
    }

    virtual bool BeginKeys(const std::vector<toft::StringPiece>& keys) {
        CHECK_NE(0u, keys.size());
        std::string key = "s";
        for (size_t i = 0; i != keys.size(); ++i) {
            CHECK(m_writer->WriteRecord(key, keys[i]));
            key = "k";
        }
        return true;
    }

    virtual bool Write(const toft::StringPiece& value) {
        CHECK_NOTNULL(m_writer.get());
        return m_writer->WriteRecord("", value);
    }

    virtual bool EndKeys() { return true; }

    bool Close() {
        if (m_closed) {
            return true;
        }
        CHECK_NOTNULL(m_writer.get());
        if (!m_writer->Close()) {
            return false;
        }
        LOG(INFO) << "Rename file from: "
            << flume::util::HideToftPathPassword(m_tmp_uri)
            << " To: "
            << flume::util::HideToftPathPassword(m_target_uri);
        if (!toft::File::Exists(m_target_uri)) {
            CHECK(toft::File::Rename(m_tmp_uri, m_target_uri))
                << "fail to rename " << m_tmp_uri
                << " to " << m_target_uri
                << ", errno = " << errno;
        }
        m_writer.reset();
        m_closed = true;
        return true;
    }

    ~Writer() {
        CHECK(Close());
    }

private:
    std::string m_uri;
    std::string m_task_id;
    std::string m_target_uri;
    std::string m_tmp_uri;
    toft::scoped_ptr<toft::LocalSequenceFileWriter> m_writer;
    bool m_closed;
};

CacheManager::Reader* FileCacheManager::GetReader(const std::string& node_id) {
    typedef FileCacheManager::Reader Reader;
    BOOST_AUTO(iter, m_readers.find(node_id));
    if (iter == m_readers.end()) {
        BOOST_AUTO(reader, boost::make_shared<Reader>(GetCachedNodePath(m_tmp_data_path, node_id)));
        BOOST_AUTO(pair, std::make_pair(node_id, reader));
        BOOST_AUTO(inserted, m_readers.insert(pair));
        CHECK(inserted.second);
        return inserted.first->second.get();
    }
    return iter->second.get();
}

CacheManager::Writer* FileCacheManager::GetWriter(const std::string& node_id) {
    typedef FileCacheManager::Writer Writer;
    BOOST_AUTO(iter, m_writers.find(node_id));
    if (iter == m_writers.end()) {
        std::string path = GetCachedNodePath(m_tmp_data_path, node_id);
        BOOST_AUTO(writer, boost::make_shared<Writer>(path, m_task_id));
        BOOST_AUTO(pair, std::make_pair(node_id, writer));
        BOOST_AUTO(inserted, m_writers.insert(pair));
        CHECK(inserted.second);
        return inserted.first->second.get();
    }
    return iter->second.get();
}

void FileCacheManager::Shutdown() {
    m_readers.clear();
    m_writers.clear();
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
