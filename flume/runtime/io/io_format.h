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
//
// Interface, load datas from a storage like file, database and so on. Corresponding to
// LOAD_NODE in logical execution plan. See wing/doc/backend.rst for details.

#ifndef FLUME_RUNTIME_IO_IO_FORMAT_H_
#define FLUME_RUNTIME_IO_IO_FORMAT_H_

#include <string>
#include <vector>

#include "toft/base/closure.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "toft/storage/file/hdfs_file.h"
#include "toft/storage/file/local_file.h"
#include "toft/storage/file/uri_utils.h"

#include "flume/core/emitter.h"
#include "flume/core/key_reader.h"
#include "flume/core/loader.h"
#include "flume/core/objector.h"
#include "flume/core/sinker.h"

namespace toft {
class LocalSequenceFileWriter;
class File;
}

namespace baidu {
namespace flume {

namespace util {
class HadoopConf;
} // namespace util

namespace runtime {

struct Record {
    toft::StringPiece key;
    toft::StringPiece value;

public:
    Record() {}
    Record(const toft::StringPiece& k, const toft::StringPiece& v) : key(k), value(v) {}
};

// object will serialize/deserialize as Record using this Objector
class RecordObjector : public core::Objector {
public:
    virtual ~RecordObjector() {}

    virtual void Setup(const std::string& config) {}

    virtual uint32_t Serialize(void* object, char* buffer, uint32_t buffer_size);

    virtual void* Deserialize(const char* buffer, uint32_t buffer_size);

    virtual void Release(void* object);
};

class RecordKeyReader : public core::KeyReader {
public:
    virtual ~RecordKeyReader() {}

    virtual void Setup(const std::string& config) {}

    virtual uint32_t ReadKey(void* object, char* buffer, uint32_t buffer_size);
};

// Read a stream of <Record> from DCE InputFormat.
class InputFormat : public core::Loader {
public:
    virtual ~InputFormat() {}

    virtual void Setup(const std::string& config) {}

    virtual void Split(const std::string& uri, std::vector<std::string>* splits);

    virtual void Load(const std::string& split, core::Emitter* emitter) {}
};

// Type alias
class TextInputFormat : public InputFormat {
public:
    TextInputFormat();
    virtual ~TextInputFormat();

public:
    typedef RecordObjector ObjectorType;

    virtual void Setup(const std::string& config);

    virtual void Load(const std::string& split, core::Emitter* emitter);

    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size);

    virtual bool Deserialize(const char* buffer, uint32_t buffer_size);

private:
    class Impl;
    toft::scoped_ptr<Impl> m_impl;
};

class TextInputFormatWithUgi : public TextInputFormat {
};

class SequenceFileAsBinaryInputFormat : public InputFormat {
public:
    SequenceFileAsBinaryInputFormat();
    virtual ~SequenceFileAsBinaryInputFormat();

public:
    typedef RecordObjector ObjectorType;

    virtual void Setup(const std::string& config);

    virtual void Load(const std::string& split, core::Emitter* emitter);

    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size);

    virtual bool Deserialize(const char* buffer, uint32_t buffer_size);

private:
    class Impl;
    toft::scoped_ptr<Impl> m_impl;
};

class SequenceFileAsBinaryInputFormatWithUgi
    : public SequenceFileAsBinaryInputFormat {
};

class StreamInputFormat : public core::Loader {
public:
    virtual ~StreamInputFormat() {}

    virtual void Setup(const std::string& config) {}

    virtual void Split(const std::string& uri, std::vector<std::string>* splits);

    virtual void Load(const std::string& split, core::Emitter* emitter) {}
};

class TextStreamInputFormat : public StreamInputFormat {
public:
    TextStreamInputFormat();
    virtual ~TextStreamInputFormat();

public:
    typedef RecordObjector ObjectorType;

    virtual void Setup(const std::string& config);

    virtual void Load(const std::string& split, core::Emitter* emitter);

    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size);

    virtual bool Deserialize(const char* buffer, uint32_t buffer_size);

private:
    class Impl;
    toft::scoped_ptr<Impl> m_impl;
};

class SequenceStreamInputFormat : public StreamInputFormat {
public:
    SequenceStreamInputFormat();
    virtual ~SequenceStreamInputFormat();

public:
    typedef RecordObjector ObjectorType;

    virtual void Setup(const std::string& config);

    virtual void Load(const std::string& split, core::Emitter* emitter);

    virtual uint32_t Serialize(char* buffer, uint32_t buffer_size);

    virtual bool Deserialize(const char* buffer, uint32_t buffer_size);

private:
    class Impl;
    toft::scoped_ptr<Impl> m_impl;
};

// Write a stream of <Record> to DCE OutputFormat
class OutputFormat : public core::Sinker {
public:
    virtual ~OutputFormat() {}
};

class OutputFormatBase : public OutputFormat, public core::Committer {
public:
    OutputFormatBase();
    virtual ~OutputFormatBase();

    virtual void Setup(const std::string& config);

    virtual void Open(const std::vector<toft::StringPiece>& keys);

    virtual void Sink(void* object);

    virtual void Close();

    virtual core::Committer* GetCommitter();

    virtual void Commit();

    virtual std::string GetCommitPath(
            const std::string& dir,
            const std::string& filename);

    virtual void Initialize(const std::string& config) = 0;

    virtual void OpenFile(const std::string& path) = 0;

    virtual void CloseFile() = 0;

    virtual void WriteRecord(const Record& record) = 0;

private:
    class Impl;
    toft::scoped_ptr<Impl> m_impl;
};

class TextOutputFormat : public OutputFormatBase {
public:
    TextOutputFormat();
    virtual ~TextOutputFormat();

    virtual std::string GetCommitPath(
            const std::string& dir,
            const std::string& filename);

    virtual void Initialize(const std::string& config);

    virtual void OpenFile(const std::string& path);

    virtual void CloseFile();

    virtual void WriteRecord(const Record& record);

private:
    toft::scoped_ptr<toft::File> m_file;
    std::string m_compression_ext;
    std::string m_record_delimiter;
    bool m_has_delimiter;
};

class SequenceFileAsBinaryOutputFormat : public OutputFormatBase {
public:
    SequenceFileAsBinaryOutputFormat();
    virtual ~SequenceFileAsBinaryOutputFormat();

    virtual void Initialize(const std::string& config);

    virtual void OpenFile(const std::string& path);

    virtual void CloseFile();

    virtual void WriteRecord(const Record& record);

private:
    toft::scoped_ptr<toft::LocalSequenceFileWriter> m_writer;
    toft::scoped_ptr<toft::File> m_file;
};

class EmptyOutputFormat : public OutputFormat {
public:
    EmptyOutputFormat() {}
    virtual ~EmptyOutputFormat() {}

    virtual void Setup(const std::string& config) { }
    virtual void Open(const std::vector<toft::StringPiece>& keys) { }
    virtual void Sink(void* object) { }
    virtual void Close() { }

};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_IO_IO_FORMAT_H_
