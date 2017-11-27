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
// All rights reserved.
//
// Author: Wang Cong <bigflow-opensource@baidu.com>

#ifndef FLUME_RUNTIME_IO_GZIP_FILE_H
#define FLUME_RUNTIME_IO_GZIP_FILE_H

#include <string>
#include <zlib.h>

#include "toft/base/scoped_array.h"
#include "toft/base/scoped_ptr.h"
#include "toft/storage/file/file.h"

#include "flume/runtime/io/compression_file_factory.h"

namespace baidu {
namespace flume {
namespace runtime {

class CompressionFileFactory;

static const char kIgnoreMe[] = "";

// A toft::File with GZip compression
class GZipFile : public toft::File {
    TOFT_DECLARE_UNCOPYABLE(GZipFile);

public:
    static GZipFile* From(toft::File* file);
    virtual ~GZipFile();

    // Implement toft::File interface.
    virtual int64_t Read(void* buffer, int64_t size);
    virtual int64_t Write(const void* buffer, int64_t size);
    virtual bool Flush();
    virtual bool Close();
    virtual bool Seek(int64_t offset, int whence);
    virtual int64_t Tell();
    virtual bool ReadLine(std::string* line, size_t max_size);
    virtual bool ReadLine(toft::StringPiece* line, size_t max_size);
    virtual bool ReadLineWithLineEnding(std::string* line, size_t max_size);
    virtual bool ReadLineWithLineEnding(toft::StringPiece* line, size_t max_size);
    virtual bool IsEof();

private:
    explicit GZipFile(toft::File* file);
    void EnlargeBuffer();
    void ResizeLineBuffer(size_t size);
    void BufferedReadLine(size_t max_size, size_t* have, bool* is_eol);
    void GZipInit();
    void GZipRelease();

private:
    bool _is_closed;
    toft::scoped_ptr<toft::File> _file;
    toft::scoped_array<char> _deflat_buffer;
    toft::scoped_array<char> _line_buffer;
    z_stream* _zlib_deflate_stream;
    z_stream* _zlib_inflate_stream;
    size_t _deflat_buffer_size;
    size_t _line_buffer_size;
    size_t _offset;

    static const int DEFAULT_ZLIB_MEM_LEVEL = 8;
    // gzip_format-31, default-15
    static const size_t DEFAULT_WINDOW_SIZE = 31;
    // In case of bugs
    static const size_t MAX_BUFFER_SIZE = 1024 * 1024;
    // this buffer size is advised in zlib.h
    static const size_t INIT_BUFFER_SIZE = 128 * 1024;
    // default-(-1), best_speed-1, best_compression-9
    static const int DEFAULT_ZIP_LEVEL = 1;
    // default-0, filtered-1, huffman-2, rle-3, fixed-4
    static const int DEFAULT_STRATEGY = 0;
};

} // namespace runtime
} // namespace flume
} // namespace baidu

#endif // FLUME_RUNTIME_IO_GZIP_FILE_H

