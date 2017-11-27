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
//         Zhou Kai <zhoukai01@baidu.com>
//         Zhang Yuncong <zhangyuncong@baidu.com>
//         Xu Yao <xuyao02@baidu.com>

#include "flume/runtime/io/io_format.h"

#include <stdlib.h>

#include <string>
#include <vector>
#include <iomanip>
#include <algorithm>

#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/thread/thread.hpp"
#include "boost/lockfree/spsc_queue.hpp"
#include "boost/atomic.hpp"
#include "toft/base/string/algorithm.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/storage/file/file.h"
#include "toft/storage/seqfile/local_sequence_file_reader.h"
#include "toft/storage/seqfile/local_sequence_file_writer.h"
#include "toft/system/threading/thread.h"
#include "toft/system/time/clock.h"

#include "flume/core/partitioner.h"
#include "flume/proto/entity.pb.h"
#include "flume/runtime/io/compression_file_factory.h"
#include "flume/util/hadoop_conf.h"
#include "flume/util/path_util.h"
#include "flume/runtime/counter.h"

namespace baidu {
namespace flume {
namespace runtime {

static const size_t kMaxTextLineLength = 64 * 1024 * 1024; // consistent with Hadoop TextInputFormat

using baidu::flume::util::IsHdfsPath;

uint32_t RecordObjector::Serialize(void* object, char* buffer, uint32_t buffer_size) {
    Record* record = static_cast<Record*>(object);
    const toft::StringPiece& key = record->key;
    const toft::StringPiece& value = record->value;
    uint32_t key_size = key.size();
    uint32_t value_size = value.size();
    uint32_t size = sizeof(key_size) + key_size + sizeof(value_size) + value_size;
    if (size <= buffer_size) {
        memcpy(buffer, &key_size, sizeof(key_size));
        buffer += sizeof(key_size);
        memcpy(buffer, key.data(), key.size());
        buffer += key.size();
        memcpy(buffer, &value_size, sizeof(value_size));
        buffer += sizeof(value_size);
        memcpy(buffer, value.data(), value.size());
    }
    return size;
}

void* RecordObjector::Deserialize(const char* buffer, uint32_t buffer_size) {
    toft::scoped_ptr<Record> record(new Record());
    const uint32_t* key_size = NULL;
    const uint32_t* value_size = NULL;
    if (buffer_size < sizeof(*key_size)) {
        return NULL;
    }
    key_size = reinterpret_cast<const uint32_t*>(buffer);
    buffer += sizeof(*key_size);
    buffer_size -= sizeof(*key_size);
    if (buffer_size < *key_size) {
        return NULL;
    }
    record->key.set(buffer, *key_size);
    buffer += *key_size;
    buffer_size -= *key_size;
    if (buffer_size < sizeof(*value_size)) {
        return NULL;
    }
    value_size = reinterpret_cast<const uint32_t*>(buffer);
    buffer += sizeof(*value_size);
    buffer_size -= sizeof(*value_size);
    if (buffer_size < *value_size) {
        return NULL;
    }
    record->value.set(buffer, *value_size);
    return record.release();
}

// TODO(wenxiang): use memory pool to void frequently new and delete.
void RecordObjector::Release(void* object) {
    Record* record = static_cast<Record*>(object);
    delete record;
}

uint32_t RecordKeyReader::ReadKey(void* object, char* buffer, uint32_t buffer_size) {
    Record* record = static_cast<Record*>(object);
    if (record->key.size() <= buffer_size) {
        memcpy(buffer, record->key.data(), record->key.size());
    }
    return record->key.size();
}

void InputFormat::Split(const std::string& uri, std::vector<std::string>* splits) {
    LOG(INFO) << "split uri : "
        << flume::util::HideToftPathPassword(uri);
    toft::FileType uri_type = toft::File::GetFileType(uri);
    if (uri_type == toft::FileType_Directory) {
        using toft::FileIterator;
        FileIterator* iter =
                toft::File::Iterate(uri, "*", toft::FileType_Regular | toft::FileType_Link);
        toft::scoped_ptr<FileIterator> iterator(iter);
        CHECK(NULL != iterator) << "List file [" << uri << "/*] failed";
        toft::FileEntry entry;
        while (iterator->GetNext(&entry)) {
            LOG(INFO) << "get file " << entry.name;
            splits->push_back(uri + "/" + entry.name);
        }
    } else if (uri_type == toft::FileType_Regular) {
        splits->push_back(uri);
    } else {
        LOG(FATAL) << "Undetermined file type for uri: "
            << flume::util::HideToftPathPassword(uri);
    }
}

class TextInputFormat::Impl {
public:
    Impl() : _inited(false), _uri(""), _offset(0) {}
    ~Impl() {}

public:
    void setup(const std::string& config);

    void load(const std::string& split, core::Emitter* emitter);

    uint32_t serialize(char* buffer, uint32_t buffer_size);

    bool deserialize(const char* buffer, uint32_t buffer_size);

private:
    bool initialize();
    void uninitialize();

private:
    PbInputFormatEntityConfig _pb_config;
    bool _inited;
    std::string _uri;
    int64_t _offset;
    toft::scoped_ptr<toft::File> _file;
};

bool TextInputFormat::Impl::initialize() {
    if (_inited) {
        return true;
    }
    _file.reset(toft::File::Open(_uri, "r"));
    if (!_file.get()) {
        LOG(ERROR) << "File open error, file: "
            << flume::util::HideToftPathPassword(_uri);
        return false;
    }
    bool compressed = false;
    BOOST_FOREACH(const std::string& ext, CompressionFileFactory::GetCompressions()) {
        if (toft::StringEndsWith(_uri, "." + ext)) {
            LOG(INFO) << "Detected compression file as by filename extention: [." << ext << "]";
            _file.reset(CompressionFileFactory::CompressionFile(_file.release(), ext));
            compressed = true;
        }
    }
    if (_pb_config.repeatedly() && _offset != 0) {
        CHECK(!compressed) << "Compression file can't be repeatedly";
        if (!_file->Seek(_offset, SEEK_SET)) {
            LOG(ERROR) << "File seek error, file: "
                << flume::util::HideToftPathPassword(_uri)
                << " offset: "
                << _offset;
            return false;
        }
    }
    _inited = true;
    return true;
}

void TextInputFormat::Impl::uninitialize() {
    if (_inited) {
        _inited = false;
    }
}

void TextInputFormat::Impl::setup(const std::string& config) {
    if (config == "use_dce_combine") {
        //do nothing
    } else {
        CHECK(_pb_config.ParseFromString(config));
    }
}

void TextInputFormat::Impl::load(const std::string& split, core::Emitter* emitter) {
    // finite dataset always initialize
    if (!_inited || !_pb_config.repeatedly()) {
        _uri = split;
        _offset = 0;
        std::string buffer(1024, '\0');
        size_t size = serialize(&(*buffer.begin()), buffer.size());
        if (size > buffer.size()) {
            buffer.resize(size);
            size_t new_size = serialize(&(*buffer.begin()), buffer.size());
            CHECK_LE(new_size, size);
            size = new_size;
        }
        buffer.resize(size);
        CHECK(deserialize(buffer.data(), buffer.size()));
    }
    Record record("", toft::StringPiece());
    if (_pb_config.repeatedly()) {
        uint64_t start = time(NULL);
        uint64_t record_num = 0;
        while (!_file->IsEof()) {
            if (record_num >= _pb_config.max_record_num_per_round()) {
                LOG(INFO) << "load break because max_record_num, record_num: " << record_num;
                record_num = 0;
                break;
            }
            uint64_t now = time(NULL);
            if (now - start >= _pb_config.timeout_per_round()) {
                LOG(INFO) << "load break because timeout, record_num: " << record_num;
                break;
            }
            if (!_file->ReadLine(&(record.value), kMaxTextLineLength)) {
                LOG(WARNING) << "read record failed, file: "
                    << flume::util::HideToftPathPassword(_uri);
                continue;
            }
            // TODO(wangkun08) Fill the key same as hadoop mapreduce TextInputFormat
            if (!emitter->Emit(&record)) {
                break;
            }
            ++record_num;
        }
        _offset = _file->Tell();
    } else {
        while (!_file->IsEof()) {
            if (!_file->ReadLine(&(record.value), kMaxTextLineLength)) {
                if (!_file->IsEof()) {
                    LOG(WARNING) << "read record failed, file: "
                        << flume::util::HideToftPathPassword(_uri);
                }
                continue;
            }
            // TODO(wangkun08) Fill the key same as hadoop mapreduce TextInputFormat
            if (!emitter->Emit(&record)) {
                break;
            }
        }
    }
    LOG(INFO) << "closing text reader";
}

uint32_t TextInputFormat::Impl::serialize(char* buffer, uint32_t buffer_size) {
    PbInputFormatStatus pb_status;
    pb_status.set_uri(_uri);
    pb_status.set_offset(_offset);

    LOG(INFO) << "serialize: " << pb_status.ShortDebugString();

    int32_t bytesize = pb_status.ByteSize();
    if (bytesize < 0) {
        LOG(ERROR) << "input format status bytesize failed: " << bytesize;
        return 0;
    }
    if (static_cast<uint32_t>(bytesize) > buffer_size) {
        LOG(WARNING) << "buffer_size is not enough, " << bytesize << ":" << buffer_size;
        return bytesize;
    }

    CHECK(pb_status.SerializeToArray(buffer, bytesize));
    return bytesize;
}

bool TextInputFormat::Impl::deserialize(const char* buffer, uint32_t buffer_size) {
    if (buffer_size == 0) {
        LOG(WARNING) << "first deserialize stauts is empty";
        return true;
    }

    PbInputFormatStatus pb_status;
    CHECK_NOTNULL(buffer);
    CHECK(pb_status.ParseFromArray(buffer, buffer_size));
    LOG(INFO) << "deserialize: " << pb_status.ShortDebugString();

    _uri = pb_status.uri();
    _offset = pb_status.offset();

    uninitialize();
    return initialize();
}

TextInputFormat::TextInputFormat() : m_impl(new TextInputFormat::Impl()) {}

TextInputFormat::~TextInputFormat() {}

void TextInputFormat::Setup(const std::string& config) {
    m_impl->setup(config);
}

void TextInputFormat::Load(const std::string& split, core::Emitter* emitter) {
    m_impl->load(split, emitter);
}

uint32_t TextInputFormat::Serialize(char* buffer, uint32_t buffer_size) {
    return m_impl->serialize(buffer, buffer_size);
}

bool TextInputFormat::Deserialize(const char* buffer, uint32_t buffer_size) {
    return m_impl->deserialize(buffer, buffer_size);
}

class SequenceFileAsBinaryInputFormat::Impl {
public:
    Impl() : _inited(false), _uri(""), _offset(0) {}
    ~Impl() {}

public:
    void setup(const std::string& config);

    void load(const std::string& split, core::Emitter* emitter);

    uint32_t serialize(char* buffer, uint32_t buffer_size);

    bool deserialize(const char* buffer, uint32_t buffer_size);

private:
    bool initialize();
    void uninitialize();

private:
    PbInputFormatEntityConfig _pb_config;
    bool _inited;
    std::string _uri;
    int64_t _offset;
    toft::scoped_ptr<toft::LocalSequenceFileReader> _reader;
};

bool SequenceFileAsBinaryInputFormat::Impl::initialize() {
    if (_inited) {
        return true;
    }
    toft::scoped_ptr<toft::File> file(toft::File::Open(_uri, "r"));
    _reader.reset(new toft::LocalSequenceFileReader(file.release()));
    if (!_reader.get()) {
        LOG(ERROR) << "File open error, file: "
            << flume::util::HideToftPathPassword(_uri);
        return false;
    }
    if (_pb_config.repeatedly() && _offset != 0) {
        if (!_reader->Seek(_offset)) {
            LOG(ERROR) << "File seek error, file: "
                << flume::util::HideToftPathPassword(_uri)
                << " offset: "
                << _offset;
            return false;
        }
    }
    _inited = true;
    return true;
}

void SequenceFileAsBinaryInputFormat::Impl::uninitialize() {
    if (_inited) {
        _inited = false;
        // Note: Do not close reader here, cause the destructor of the reader will close it automaticly.
        //      If you close it here, an double-close exception will occur if you are reading hdfs file.
        //      We will fix it in the LocalSequenceFileReader later
        // _reader->Close();
    }
}

void SequenceFileAsBinaryInputFormat::Impl::setup(const std::string& config) {
    if (!config.empty() && config != "use_dce_combine") {
        CHECK(_pb_config.ParseFromString(config));
    }
}

void SequenceFileAsBinaryInputFormat::Impl::load(
        const std::string& split,
        core::Emitter* emitter) {
    // finite dataset always initialize
    if (!_inited || !_pb_config.repeatedly()) {
        _uri = split;
        _offset = 0;
        std::string buffer(1024, '\0');
        size_t size = serialize(&(*buffer.begin()), buffer.size());
        if (size > buffer.size()) {
            buffer.resize(size);
            size_t new_size = serialize(&(*buffer.begin()), buffer.size());
            CHECK_LE(new_size, size);
            size = new_size;
        }
        buffer.resize(size);
        CHECK(deserialize(buffer.data(), buffer.size()));
    }
    toft::StringPiece key;
    toft::StringPiece value;
    bool is_emit_false = false;
    if (_pb_config.repeatedly()) {
        uint64_t start = time(NULL);
        uint64_t record_num = 0;
        while (_reader->ReadRecord(&key, &value)) {
            Record record(key, value);
            if (!emitter->Emit(&record)) {
                is_emit_false = true;
                break;
            }
            if (record_num >= _pb_config.max_record_num_per_round()) {
                LOG(INFO) << "load break because max_record_num, record_num: " << record_num;
                record_num = 0;
                is_emit_false = true;
                break;
            }
            uint64_t now = time(NULL);
            if (now - start >= _pb_config.timeout_per_round()) {
                LOG(INFO) << "load break because timeout, record_num: " << record_num;
                is_emit_false = true;
                break;
            }
            ++record_num;
        }
        if (!is_emit_false && !_reader->is_finished()) {
            LOG(ERROR) << "File reading error : "
                << flume::util::HideToftPathPassword(_uri);
            CHECK(0);
        }
        _offset = _reader->Tell();
    } else {
        while (_reader->ReadRecord(&key, &value)) {
            Record record(key, value);
            if (!emitter->Emit(&record)) {
                is_emit_false = true;
                break;
            }
        }
        if (!is_emit_false && !_reader->is_finished()) {
            LOG(ERROR) << "File reading error : "
                << flume::util::HideToftPathPassword(_uri);
            CHECK(0);
        }
    }
    LOG(INFO) << "closing sequence reader";
}

uint32_t SequenceFileAsBinaryInputFormat::Impl::serialize(char* buffer, uint32_t buffer_size) {
    PbInputFormatStatus pb_status;
    pb_status.set_uri(_uri);
    pb_status.set_offset(_offset);

    LOG(INFO) << "serialize: " << pb_status.ShortDebugString();

    int32_t bytesize = pb_status.ByteSize();
    if (bytesize < 0) {
        LOG(ERROR) << "input format status bytesize failed: " << bytesize;
        return 0;
    }
    if (static_cast<uint32_t>(bytesize) > buffer_size) {
        LOG(WARNING) << "buffer_size is not enough, " << bytesize << ":" << buffer_size;
        return bytesize;
    }

    CHECK(pb_status.SerializeToArray(buffer, bytesize));
    return bytesize;
}

bool SequenceFileAsBinaryInputFormat::Impl::deserialize(const char* buffer, uint32_t buffer_size) {
    if (buffer_size == 0) {
        LOG(WARNING) << "first deserialize stauts is empty";
        return true;
    }

    PbInputFormatStatus pb_status;
    CHECK_NOTNULL(buffer);
    CHECK(pb_status.ParseFromArray(buffer, buffer_size));
    LOG(INFO) << "deserialize: " << pb_status.ShortDebugString();

    _uri = pb_status.uri();
    _offset = pb_status.offset();

    uninitialize();
    return initialize();
}

SequenceFileAsBinaryInputFormat::SequenceFileAsBinaryInputFormat() :
        m_impl(new SequenceFileAsBinaryInputFormat::Impl()) {
}

SequenceFileAsBinaryInputFormat::~SequenceFileAsBinaryInputFormat() {}

void SequenceFileAsBinaryInputFormat::Setup(const std::string& config) {
    m_impl->setup(config);
}

void SequenceFileAsBinaryInputFormat::Load(const std::string& split, core::Emitter* emitter) {
    m_impl->load(split, emitter);
}

uint32_t SequenceFileAsBinaryInputFormat::Serialize(char* buffer, uint32_t buffer_size) {
    return m_impl->serialize(buffer, buffer_size);
}

bool SequenceFileAsBinaryInputFormat::Deserialize(const char* buffer, uint32_t buffer_size) {
    return m_impl->deserialize(buffer, buffer_size);
}

void StreamInputFormat::Split(const std::string& uri, std::vector<std::string>* splits) {
    LOG(INFO) << "split uri : "
        << flume::util::HideToftPathPassword(uri);
    toft::FileType uri_type = toft::File::GetFileType(uri);
    if (uri_type == toft::FileType_Directory) {
        std::vector<std::string> uri_splits;
        using toft::FileIterator;
        FileIterator* iter = toft::File::Iterate(uri, "*", toft::FileType_Directory);
        toft::scoped_ptr<FileIterator> iterator(iter);
        CHECK(NULL != iterator) << "List file [" << uri << "/*] failed";
        toft::FileEntry entry;
        while (iterator->GetNext(&entry)) {
            PbInputFormatStatus pb_status;
            pb_status.set_uri(uri + "/" + entry.name + "/");
            pb_status.set_offset(0);
            std::string split_string;
            CHECK(pb_status.SerializeToString(&split_string));
            uri_splits.push_back(split_string);
        }
        if (uri_splits.empty()) {
            PbInputFormatStatus pb_status;
            pb_status.set_uri(uri + "/");
            pb_status.set_offset(0);
            std::string split_string;
            CHECK(pb_status.SerializeToString(&split_string));
            uri_splits.push_back(split_string);
        }
        std::copy(uri_splits.begin(), uri_splits.end(), std::back_inserter(*splits));
    } else {
        LOG(FATAL) << "Undetermined file type for uri: "
            << flume::util::HideToftPathPassword(uri);
    }
}

std::string get_next_uri(const std::string& pattern, const std::string& uri) {
    std::size_t found = uri.rfind("/");
    CHECK(found != std::string::npos) << "Invalid uri: " << uri;
    std::string result;
    if (pattern == "hadoop") {
        std::stringstream ss_index(uri.substr(found + 6)); // filter "/part-"
        uint64_t index;
        ss_index >> index;
        std::stringstream ss;
        ss << std::setfill('0') << std::setw(5) << index + 1;
        result = uri.substr(0, found) + "/part-" + ss.str();
    } else {
        std::stringstream ss_index(uri.substr(found + 1)); // filter "/"
        uint64_t index;
        ss_index >> index;
        std::stringstream ss;
        ss << index + 1;
        result = uri.substr(0, found) + "/" + ss.str();
    }
    return result;
}

class TextStreamInputFormat::Impl {
public:
    Impl() : _inited(false), _existed(false), _uri(""), _offset(0) {}
    ~Impl() {}

public:
    void setup(const std::string& config);

    void load(const std::string& split, core::Emitter* emitter);

    uint32_t serialize(char* buffer, uint32_t buffer_size);

    bool deserialize(const char* buffer, uint32_t buffer_size);

private:
    bool initialize();
    void uninitialize();

private:
    PbInputFormatEntityConfig _pb_config;
    bool _inited;
    bool _existed;
    std::string _uri;
    int64_t _offset;
    toft::scoped_ptr<toft::File> _file;
};

bool TextStreamInputFormat::Impl::initialize() {
    if (_inited && _existed) {
        return true;
    }
    if (toft::File::Exists(_uri)) {
        _file.reset(toft::File::Open(_uri, "r"));
        if (!_file.get()) {
            LOG(ERROR) << "File open error, file: "
                << flume::util::HideToftPathPassword(_uri);
            return false;
        }
        if (_offset != 0) {
            if (!_file->Seek(_offset, SEEK_SET)) {
                LOG(ERROR) << "File seek error, file: "
                    << flume::util::HideToftPathPassword(_uri)
                    << " offset: "
                    << _offset;
                return false;
            }
        }
        _existed = true;
    }
    _inited = true;
    return true;
}

void TextStreamInputFormat::Impl::uninitialize() {
    if (_inited) {
        _inited = false;
    }
}

void TextStreamInputFormat::Impl::setup(const std::string& config) {
    CHECK(_pb_config.ParseFromString(config));
    CHECK(_pb_config.repeatedly());
}

void TextStreamInputFormat::Impl::load(const std::string& split, core::Emitter* emitter) {
    // finite dataset always initialize
    if (!_inited) {
        CHECK(deserialize(split.data(), split.size()));
    }
    Record record("", toft::StringPiece());
    bool existed = false;
    uint64_t record_num = 0;
    uint64_t start = time(NULL);
    while (true) {
        CHECK(initialize());
        if (!_existed) {
            LOG(WARNING) << "peek timeout, uri: "
                << flume::util::HideToftPathPassword(_uri);
            usleep(1000000);
        } else {
            if (_file->ReadLine(&(record.value), kMaxTextLineLength)) {
                if (!emitter->Emit(&record)) {
                    break;
                }
                ++record_num;
            } else {
                if (!_file->IsEof()) {
                    LOG(WARNING) << "read record failed, file: "
                        << flume::util::HideToftPathPassword(_uri);
                    continue;
                } else {
                    _existed = false;
                    _uri = get_next_uri(_pb_config.file_stream().filename_pattern(), _uri);
                    _offset = 0;
                }
            }
        }
        if (record_num >= _pb_config.max_record_num_per_round()) {
            LOG(INFO) << "load break because max_record_num, record_num: " << record_num;
            record_num = 0;
            break;
        }
        uint64_t now = time(NULL);
        if (now - start >= _pb_config.timeout_per_round()) {
            LOG(INFO) << "load break because timeout, record_num: " << record_num;
            break;
        }
    }
    if (_existed) {
        _offset = _file->Tell();
    }
    LOG(INFO) << "closing text reader";
}

uint32_t TextStreamInputFormat::Impl::serialize(char* buffer, uint32_t buffer_size) {
    PbInputFormatStatus pb_status;
    pb_status.set_uri(_uri);
    pb_status.set_offset(_offset);

    LOG(INFO) << "serialize: " << pb_status.ShortDebugString();

    int32_t bytesize = pb_status.ByteSize();
    if (bytesize < 0) {
        LOG(ERROR) << "input format status bytesize failed: " << bytesize;
        return 0;
    }
    if (static_cast<uint32_t>(bytesize) > buffer_size) {
        LOG(WARNING) << "buffer_size is not enough, " << bytesize << ":" << buffer_size;
        return bytesize;
    }

    CHECK(pb_status.SerializeToArray(buffer, bytesize));
    return bytesize;
}

bool TextStreamInputFormat::Impl::deserialize(const char* buffer, uint32_t buffer_size) {
    if (buffer_size == 0) {
        LOG(WARNING) << "first deserialize stauts is empty";
        return true;
    }

    PbInputFormatStatus pb_status;
    CHECK_NOTNULL(buffer);
    CHECK(pb_status.ParseFromArray(buffer, buffer_size));
    LOG(INFO) << "deserialize: " << pb_status.ShortDebugString();

    _uri = pb_status.uri();
    if (*_uri.rbegin() == '/') {
        if (_pb_config.file_stream().filename_pattern() == "hadoop") {
            _uri += "part-00000";
        } else {
            _uri += "0";
        }
    }
    _offset = pb_status.offset();

    uninitialize();
    return initialize();
}

TextStreamInputFormat::TextStreamInputFormat() : m_impl(new TextStreamInputFormat::Impl()) {}

TextStreamInputFormat::~TextStreamInputFormat() {}

void TextStreamInputFormat::Setup(const std::string& config) {
    m_impl->setup(config);
}

void TextStreamInputFormat::Load(const std::string& split, core::Emitter* emitter) {
    m_impl->load(split, emitter);
}

uint32_t TextStreamInputFormat::Serialize(char* buffer, uint32_t buffer_size) {
    return m_impl->serialize(buffer, buffer_size);
}

bool TextStreamInputFormat::Deserialize(const char* buffer, uint32_t buffer_size) {
    return m_impl->deserialize(buffer, buffer_size);
}

class SequenceStreamInputFormat::Impl {
public:
    Impl() : _inited(false), _existed(false), _uri(""), _offset(0) {}
    ~Impl() {}

public:
    void setup(const std::string& config);

    void load(const std::string& split, core::Emitter* emitter);

    uint32_t serialize(char* buffer, uint32_t buffer_size);

    bool deserialize(const char* buffer, uint32_t buffer_size);

private:
    bool initialize();
    void uninitialize();

private:
    PbInputFormatEntityConfig _pb_config;
    bool _inited;
    bool _existed;
    std::string _uri;
    int64_t _offset;
    toft::scoped_ptr<toft::LocalSequenceFileReader> _reader;
};

bool SequenceStreamInputFormat::Impl::initialize() {
    if (_inited && _existed) {
        return true;
    }
    if (toft::File::Exists(_uri)) {
        toft::scoped_ptr<toft::File> file(toft::File::Open(_uri, "r"));
        _reader.reset(new toft::LocalSequenceFileReader(file.release()));
        if (!_reader.get()) {
            LOG(ERROR) << "File open error, file: "
                << flume::util::HideToftPathPassword(_uri);
            return false;
        }
        if (_offset != 0) {
            if (!_reader->Seek(_offset)) {
                LOG(ERROR) << "File seek error, file: "
                    << flume::util::HideToftPathPassword(_uri)
                    << " offset: "
                    << _offset;
                return false;
            }
        }
        _existed = true;
    }
    _inited = true;
    return true;
}

void SequenceStreamInputFormat::Impl::uninitialize() {
    if (_inited) {
        _inited = false;
        // Note: Do not close reader here, cause the destructor of the reader will close it automaticly.
        //      If you close it here, an double-close exception will occur if you are reading hdfs file.
        //      We will fix it in the LocalSequenceFileReader later
        // _reader->Close();
    }
}

void SequenceStreamInputFormat::Impl::setup(const std::string& config) {
    CHECK(_pb_config.ParseFromString(config));
    CHECK(_pb_config.repeatedly());
}

void SequenceStreamInputFormat::Impl::load(const std::string& split, core::Emitter* emitter) {
    // finite dataset always initialize
    if (!_inited) {
        CHECK(deserialize(split.data(), split.size()));
    }
    toft::StringPiece key;
    toft::StringPiece value;
    bool existed = false;
    uint64_t record_num = 0;
    uint64_t start = time(NULL);
    while (true) {
        CHECK(initialize());
        if (!_existed) {
            LOG(WARNING) << "peek timeout, uri: "
                << flume::util::HideToftPathPassword(_uri);
            usleep(1000000);
        } else {
            if (_reader->ReadRecord(&key, &value)) {
                Record record(key, value);
                if (!emitter->Emit(&record)) {
                    break;
                }
                ++record_num;
            } else {
                if (!_reader->is_finished()) {
                    LOG(ERROR) << "File reading error : "
                        << flume::util::HideToftPathPassword(_uri);
                    CHECK(0);
                } else {
                    _existed = false;
                    _uri = get_next_uri(_pb_config.file_stream().filename_pattern(), _uri);
                    _offset = 0;
                }
            }
        }
        if (record_num >= _pb_config.max_record_num_per_round()) {
            LOG(INFO) << "load break because max_record_num, record_num: " << record_num;
            record_num = 0;
            break;
        }
        uint64_t now = time(NULL);
        if (now - start >= _pb_config.timeout_per_round()) {
            LOG(INFO) << "load break because timeout, record_num: " << record_num;
            break;
        }
    }
    if (_existed) {
        _offset = _reader->Tell();
    }
    LOG(INFO) << "closing text reader";
}

uint32_t SequenceStreamInputFormat::Impl::serialize(char* buffer, uint32_t buffer_size) {
    PbInputFormatStatus pb_status;
    pb_status.set_uri(_uri);
    pb_status.set_offset(_offset);

    LOG(INFO) << "serialize: " << pb_status.ShortDebugString();

    int32_t bytesize = pb_status.ByteSize();
    if (bytesize < 0) {
        LOG(ERROR) << "input format status bytesize failed: " << bytesize;
        return 0;
    }
    if (static_cast<uint32_t>(bytesize) > buffer_size) {
        LOG(WARNING) << "buffer_size is not enough, " << bytesize << ":" << buffer_size;
        return bytesize;
    }

    CHECK(pb_status.SerializeToArray(buffer, bytesize));
    return bytesize;
}

bool SequenceStreamInputFormat::Impl::deserialize(const char* buffer, uint32_t buffer_size) {
    if (buffer_size == 0) {
        LOG(WARNING) << "first deserialize stauts is empty";
        return true;
    }

    PbInputFormatStatus pb_status;
    CHECK_NOTNULL(buffer);
    CHECK(pb_status.ParseFromArray(buffer, buffer_size));
    LOG(INFO) << "deserialize: " << pb_status.ShortDebugString();

    _uri = pb_status.uri();
    if (*_uri.rbegin() == '/') {
        if (_pb_config.file_stream().filename_pattern() == "hadoop") {
            _uri += "part-00000";
        } else {
            _uri += "0";
        }
    }
    _offset = pb_status.offset();

    uninitialize();
    return initialize();
}

SequenceStreamInputFormat::SequenceStreamInputFormat() :
        m_impl(new SequenceStreamInputFormat::Impl()) {}

SequenceStreamInputFormat::~SequenceStreamInputFormat() {}

void SequenceStreamInputFormat::Setup(const std::string& config) {
    m_impl->setup(config);
}

void SequenceStreamInputFormat::Load(const std::string& split, core::Emitter* emitter) {
    m_impl->load(split, emitter);
}

uint32_t SequenceStreamInputFormat::Serialize(char* buffer, uint32_t buffer_size) {
    return m_impl->serialize(buffer, buffer_size);
}

bool SequenceStreamInputFormat::Deserialize(const char* buffer, uint32_t buffer_size) {
    return m_impl->deserialize(buffer, buffer_size);
}

class OutputFormatBase::Impl : public core::Sinker, public core::Committer {
public:
    Impl(OutputFormatBase* output_format);
    virtual ~Impl();

public:
    virtual void Setup(const std::string& config);

    virtual void Open(const std::vector<toft::StringPiece>& keys);

    virtual void Sink(void* object);

    virtual void Close();

    virtual core::Committer* GetCommitter();

    virtual void Commit();

private:
    struct RealRecord {
        std::string key;
        std::string value;

    public:
        RealRecord() {}
        RealRecord(const Record& record) :
            key(record.key.data(), record.key.size()),
            value(record.value.data(), record.value.size()) {}
    };

private:
    uint64_t GetWaitingTime(uint32_t exponent_counter);
    void WriteRealRecord();
    void WriteRealRecordThread();

private:
    bool m_async_mode;
    bool m_overwrite;
    std::string m_commit_dir_path;
    std::string m_dir_path;
    std::string m_commit_path;
    std::string m_temp_path;
    OutputFormatBase* m_output_format;

    RealRecord* m_record_array[8192];
    boost::lockfree::spsc_queue<RealRecord*, boost::lockfree::capacity<8192> > m_record_queue;
    boost::atomic<bool> m_sink_done;

    toft::scoped_ptr<toft::Thread> m_sink_thread;

    uint64_t m_max_waiting_time;
    uint64_t m_min_waiting_time;
    uint32_t m_write_exponent_counter;
    uint32_t m_read_exponent_counter;
};

OutputFormatBase::Impl::Impl(OutputFormatBase* output_format) :
    m_output_format(output_format) {
}

OutputFormatBase::Impl::~Impl() {}

void OutputFormatBase::Impl::Setup(const std::string& config) {
    PbOutputFormatEntityConfig pb_config;
    CHECK(pb_config.ParseFromString(config));
    m_async_mode = pb_config.async_mode();
    m_overwrite = pb_config.overwrite();
    m_dir_path = pb_config.path();
    m_commit_dir_path = pb_config.has_commit_path() ? pb_config.commit_path() : "";
    m_max_waiting_time = pb_config.has_max_waiting_time() ? pb_config.max_waiting_time() : 4096;
    m_min_waiting_time = pb_config.has_min_waiting_time() ? pb_config.min_waiting_time() : 128;

    m_output_format->Initialize(pb_config.output_format_config());
}

void OutputFormatBase::Impl::Open(const std::vector<toft::StringPiece>& keys) {
    std::string file_name = "part";
    CHECK(keys.size() == 1 || keys.size() == 2 || keys.size() == 3)
            << "Can not place OutputFormat in nested scope! scope_level = " << keys.size() - 1;
    if (keys.size() == 2 || keys.size() == 3) {
        // OutputFormat is under BUCKET scope
        uint32_t partition = core::DecodePartition(keys[1]);
        std::ostringstream ostr;
        ostr << std::setfill('0') << std::setw(5) << partition;
        file_name += "-" + ostr.str();
    }
    if (keys.size() == 3) {
        uint32_t index = core::DecodePartition(keys[2]);
        std::ostringstream ostr;
        ostr << index;
        file_name += "-" + ostr.str();
    }

    m_commit_path = m_output_format->GetCommitPath(m_dir_path, file_name);
    std::string temp_dir = m_dir_path + "/_tmp/";
    m_temp_path =
            temp_dir + file_name + "." + keys[0].as_string();  // append attemp id to temp path

    if (!IsHdfsPath(temp_dir)) {
        std::string cmd = "mkdir -p \'" + temp_dir + "\'";
        CHECK_EQ(0, system(cmd.c_str()));
    }

    LOG(INFO) << "open tmp file " << m_temp_path;
    m_output_format->OpenFile(m_temp_path);

    if (m_async_mode) {
        m_write_exponent_counter = 0;
        m_read_exponent_counter= 0;

        m_sink_done = false;
        m_sink_thread.reset(new toft::Thread());
        m_sink_thread->Start(boost::bind(&OutputFormatBase::Impl::WriteRealRecordThread, this));
    }
}

void OutputFormatBase::Impl::Sink(void* object) {
    Record* record = static_cast<Record*>(object);
    if (m_async_mode) {
        RealRecord* real_record = new RealRecord(*record);
        while (!m_record_queue.push(real_record)) {
            LOG(INFO) << "record queue ringbuffer is full, take a rest";
            usleep(GetWaitingTime(m_write_exponent_counter++));
        }
        m_write_exponent_counter = 0;
    } else {
        m_output_format->WriteRecord(*record);
    }
}

void OutputFormatBase::Impl::Close() {
    if (m_async_mode) {
        m_sink_done = true;
        CHECK(m_sink_thread->Join());

        WriteRealRecord();
    }

    m_output_format->CloseFile();
    // if target file exists, just return ok
    if (!toft::File::Exists(m_commit_path)) {
        CHECK(toft::File::Rename(m_temp_path, m_commit_path))
            << "rename [" << m_temp_path << "] to [" << m_commit_path << "] failed, "
            << "errno = " << errno;
    }
}

uint64_t OutputFormatBase::Impl::GetWaitingTime(uint32_t exponent_counter) {
    uint64_t waiting_time = m_min_waiting_time;
    uint32_t counter = std::min(exponent_counter, 32u);

    return std::min(m_min_waiting_time << counter, m_max_waiting_time);
}

void OutputFormatBase::Impl::WriteRealRecord() {
    Record record;
    RealRecord* real_record = NULL;
    size_t pop_size = 0;
    size_t array_size = sizeof(m_record_array) / sizeof(m_record_array[0]);
    pop_size = m_record_queue.pop(m_record_array, array_size);
    if (pop_size == 0) {
        DLOG(INFO) << "record queue ringbuffer is empty, take a rest";
        usleep(GetWaitingTime(m_read_exponent_counter++));
    } else {
        DLOG(INFO) << "get records from record queue, record_size: "<< pop_size;
        m_read_exponent_counter = 0;
        for (size_t i = 0; i < pop_size; ++i) {
            real_record = m_record_array[i];
            record.key.set(real_record->key);
            record.value.set(real_record->value);

            m_output_format->WriteRecord(record);
            if (real_record != NULL) {
                delete real_record;
                real_record = NULL;
            }
        }
    }
}

void OutputFormatBase::Impl::WriteRealRecordThread() {
    LOG(INFO) << "WriteRealRecordThread begin...";
    if (m_async_mode) {
        while (!m_sink_done) {
            WriteRealRecord();
        }
    }
    LOG(INFO) << "WriteRealRecordThread end...";
}

core::Committer* OutputFormatBase::Impl::GetCommitter() {
    return m_output_format;
}

void OutputFormatBase::Impl::Commit() {
    // delete tmp dir
    std::string temp_dir = m_dir_path + "/_tmp/";
    if (IsHdfsPath(temp_dir)) {
        CHECK(toft::File::Delete(temp_dir))
            << " delete [" << temp_dir << "] failed, errno = " << errno;
    } else {
        std::string cmd = "rm -rf \'";
        cmd += temp_dir + "\'";
        CHECK_EQ(0, system(cmd.c_str()));
    }

    // if target dirctory is empty, just return
    if (m_commit_dir_path.empty()) {
        return;
    }
    if (toft::File::Exists(m_commit_dir_path)) {
        if (m_overwrite) {
            if (flume::util::IsHdfsPath(m_commit_dir_path)) {
                CHECK(toft::File::Delete(m_commit_dir_path))
                    << " delete [" << m_commit_dir_path << "] failed, errno = " << errno;
            } else {
                std::string cmd = "rm -rf \'";
                cmd += m_commit_dir_path + "\'";
                CHECK_EQ(0, system(cmd.c_str()));
            }
        } else {
            CHECK(false) << "commit [" << m_commit_dir_path << "] failed, the dir is existed";
        }
    }
    CHECK(toft::File::Rename(m_dir_path, m_commit_dir_path))
        << "rename [" << m_dir_path << "] to [" << m_commit_dir_path << "] failed, "
        << "errno = " << errno;
    LOG(INFO) << "commit [" << m_dir_path << "] to [" << m_commit_dir_path << "]";
}

OutputFormatBase::OutputFormatBase() : m_impl(new OutputFormatBase::Impl(this)) {}

OutputFormatBase::~OutputFormatBase() {}

void OutputFormatBase::Setup(const std::string& config) {
    m_impl->Setup(config);
}

void OutputFormatBase::Open(const std::vector<toft::StringPiece>& keys) {
    m_impl->Open(keys);
}

void OutputFormatBase::Sink(void* object) {
    m_impl->Sink(object);
}

void OutputFormatBase::Close() {
    m_impl->Close();
}

core::Committer* OutputFormatBase::GetCommitter() {
    return m_impl->GetCommitter();
}

void OutputFormatBase::Commit() {
    m_impl->Commit();
}

std::string OutputFormatBase::GetCommitPath(
        const std::string& dir,
        const std::string& filename) {
    return dir + "/" + filename;
}

TextOutputFormat::TextOutputFormat() {}

TextOutputFormat::~TextOutputFormat() {}

void TextOutputFormat::Initialize(const std::string& config) {
    PbTextOutputFormatConfig pb_config;
    CHECK(pb_config.ParseFromString(config));
    switch (pb_config.compression_type()) {
        case PbTextOutputFormatConfig::NONE: {
            m_compression_ext.clear();
            break;
            }
        case PbTextOutputFormatConfig::GZIP: {
            m_compression_ext = "gz";
        }
    }

    if (pb_config.has_record_delimiter()) {
        m_has_delimiter = true;
        m_record_delimiter = pb_config.record_delimiter();
    } else {
        m_has_delimiter = false;
    }
}

std::string TextOutputFormat::GetCommitPath(
        const std::string& dir,
        const std::string& filename) {
    if (m_compression_ext.empty()) {
        return dir + "/" + filename;
    } else {
        return dir + "/" + filename + "." + m_compression_ext;
    }
}

void TextOutputFormat::WriteRecord(const Record& record) {
    CHECK_NOTNULL(m_file.get());
    if (record.key.size() != 0) {
        m_file->Write(record.key.data(), record.key.size());
        // If key or value is empty, skip separator
        if (record.value.size() != 0) {
            char tab = '\t';
            m_file->Write(&tab, sizeof(tab));
        }
    }
    if (record.value.size() != 0) {
        m_file->Write(record.value.data(), record.value.size());
    }
    if (m_has_delimiter) {
        m_file->Write(m_record_delimiter.data(), m_record_delimiter.length());
    }
}

void TextOutputFormat::OpenFile(const std::string& path) {
    m_file.reset(toft::File::Open(path, "w"));
    CHECK(m_file.get() != NULL) << "open [" << path <<"] failed";

    if (!m_compression_ext.empty()) {
        m_file.reset(CompressionFileFactory::CompressionFile(m_file.release(), m_compression_ext));
        CHECK(m_file.get() != NULL) << "open compression file [" << path << "] failed";
    }
}

void TextOutputFormat::CloseFile() {
    CHECK_NOTNULL(m_file.get());

    CHECK(m_file->Flush())
        << "Flush failed, current errno: " << errno << ", errstr: " << strerror(errno);

    CHECK(m_file->Close())
        << "Close failed, current errno: " << errno << ", errstr: " << strerror(errno);

    m_file.reset();
}

SequenceFileAsBinaryOutputFormat::SequenceFileAsBinaryOutputFormat() {}

SequenceFileAsBinaryOutputFormat::~SequenceFileAsBinaryOutputFormat() {}

void SequenceFileAsBinaryOutputFormat::Initialize(const std::string& config) {}

void SequenceFileAsBinaryOutputFormat::OpenFile(const std::string& path) {
    m_file.reset(toft::File::Open(path, "w"));
    m_writer.reset(new toft::LocalSequenceFileWriter(m_file.release()));
    CHECK_NOTNULL(m_writer.get());
    CHECK(m_writer->Init());
}

void SequenceFileAsBinaryOutputFormat::CloseFile() {
    CHECK_NOTNULL(m_writer.get());
    CHECK(m_writer->Close())
        << "Close failed, current errno: " << errno << ", errstr: " << strerror(errno);
    m_writer.reset();
    m_file.reset();
}

void SequenceFileAsBinaryOutputFormat::WriteRecord(const Record& record) {
    CHECK_NOTNULL(m_writer.get());
    m_writer->WriteRecord(record.key, record.value);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
