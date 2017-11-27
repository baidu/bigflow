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
// Author: Wang, Cong <wangcong09@baidu.com>

#include "flume/runtime/io/gzip_file.h"

#include "glog/logging.h"

namespace baidu {
namespace flume {
namespace runtime {

GZipFile* GZipFile::From(toft::File* file) {
    CHECK_NOTNULL(file);
    toft::scoped_ptr<GZipFile> gzip_file(new GZipFile(file));
    return gzip_file.release();
}

GZipFile::GZipFile(toft::File* file) :
        File(kIgnoreMe, kIgnoreMe),
        _is_closed(false),
        _zlib_deflate_stream(NULL),
        _zlib_inflate_stream(NULL),
        _deflat_buffer_size(INIT_BUFFER_SIZE),
        _line_buffer_size(0),
        _offset(0) {
    _file.reset(file);
    _deflat_buffer.reset(new char[INIT_BUFFER_SIZE]);
    GZipInit();
}

GZipFile::~GZipFile() {
    Close();
}

void GZipFile::GZipInit() {
    _zlib_deflate_stream = reinterpret_cast<z_stream*>(malloc(sizeof(z_stream)));
    CHECK_NOTNULL(_zlib_deflate_stream);
    memset(_zlib_deflate_stream, 0, sizeof(z_stream));
    int ret = deflateInit2(
            _zlib_deflate_stream,
            Z_DEFAULT_COMPRESSION, Z_DEFLATED,
            DEFAULT_WINDOW_SIZE,
            DEFAULT_ZLIB_MEM_LEVEL,
            DEFAULT_STRATEGY);

    CHECK_EQ(ret, Z_OK) << "Compression initialization failed!";

    _zlib_inflate_stream = reinterpret_cast<z_stream*>(malloc(sizeof(z_stream)));
    CHECK_NOTNULL(_zlib_inflate_stream);
    memset(_zlib_inflate_stream, 0, sizeof(z_stream));
    ret = inflateInit2(
            _zlib_inflate_stream,
            DEFAULT_WINDOW_SIZE);

    CHECK_EQ(ret, Z_OK) << "Decompression initialization failed!";
}

int64_t GZipFile::Write(const void* buffer, int64_t size) {
    if (size <= 0) {
        return 0u;
    }
    size_t length = static_cast<size_t>(size);
    _zlib_deflate_stream->next_in = reinterpret_cast<Bytef*>(const_cast<void*>(buffer));
    _zlib_deflate_stream->avail_in = length;

    int64_t total = 0;
    while (true) {
        // avail_out and next_out must be updated by our own
        _zlib_deflate_stream->next_out = reinterpret_cast<Bytef*>(_deflat_buffer.get());
        _zlib_deflate_stream->avail_out = _deflat_buffer_size;

        int ret = deflate(_zlib_deflate_stream, Z_NO_FLUSH);
        CHECK_NE(ret, Z_STREAM_ERROR);

        int have = _deflat_buffer_size - _zlib_deflate_stream->avail_out;
        if (have > 0) {
            int64_t part = _file->Write(_deflat_buffer.get(), have);
            total += part;
        }
        if (_zlib_deflate_stream->avail_out > 0u) {
            CHECK_EQ(_zlib_deflate_stream->avail_in, 0u); // All input consumed
            break;
        } else {
            EnlargeBuffer();
        }
    }
    return total;
}

bool GZipFile::Flush() {
    return _file->Flush();
}

bool GZipFile::Close() {
    if (_is_closed) {
        return false;
    } else {
        _is_closed = true;
        GZipRelease();
        Flush();
        return _file->Close();
    }
}

int64_t GZipFile::Read(void* buffer, int64_t size) {
    _zlib_inflate_stream->avail_out = size;
    _zlib_inflate_stream->next_out = reinterpret_cast<Bytef *>(buffer);
    int mode = Z_NO_FLUSH;

    do {
        if (_zlib_inflate_stream->avail_in == 0) {
            // All deflat_buffer consumed, try reading more
            long read_size = _file->Read(_deflat_buffer.get(), INIT_BUFFER_SIZE);
            if (read_size < 0) {
                read_size = 0;
                mode = Z_SYNC_FLUSH;
            }
            _zlib_inflate_stream->avail_in = static_cast<size_t>(read_size);
            _zlib_inflate_stream->next_in = reinterpret_cast<Bytef*>(_deflat_buffer.get());
        }
        int ret = inflate(_zlib_inflate_stream, mode);
        if (ret == Z_STREAM_END) {
            // EOF
            CHECK_GE(size, _zlib_inflate_stream->avail_out);
            int64_t have = size - _zlib_inflate_stream->avail_out;
            _offset += have;
            return have;
        }
        CHECK_EQ(ret, Z_OK);
    } while (_zlib_inflate_stream->avail_out > 0);

    _offset += size;
    return size;
}

bool GZipFile::Seek(int64_t offset, int whence) {
    // Not implemented yet
    return false;
}

int64_t GZipFile::Tell() {
    return _offset;
}

bool GZipFile::ReadLine(std::string* line, size_t max_size) {
    if (NULL == line) {
        return false;
    }
    size_t have = 0u;
    bool is_eol = false;
    BufferedReadLine(max_size, &have, &is_eol);
    if (0u == have && !is_eol) {
        return false;
    }
    line->assign(_line_buffer.get(), have);
    return true;
}

bool GZipFile::ReadLine(toft::StringPiece* line, size_t max_size) {
    if (NULL == line) {
        return false;
    }
    size_t have = 0u;
    bool is_eol = false;
    BufferedReadLine(max_size, &have, &is_eol);
    if (0u == have && !is_eol) {
        return false;
    }
    line->set(_line_buffer.get(), have);
    return true;
}

bool GZipFile::ReadLineWithLineEnding(std::string* line, size_t max_size) {
    if (NULL == line) {
        return false;
    }
    size_t have = 0u;
    bool is_eol = false;
    BufferedReadLine(max_size, &have, &is_eol);
    if (0u == have && !is_eol) {
        return false;
    }
    if (is_eol) {
        ++have;
    }
    line->assign(_line_buffer.get(), have);
    return true;
}

bool GZipFile::ReadLineWithLineEnding(toft::StringPiece* line, size_t max_size) {
    if (NULL == line) {
        return false;
    }
    size_t have = 0u;
    bool is_eol = false;
    BufferedReadLine(max_size, &have, &is_eol);
    if (0u == have && !is_eol) {
        return false;
    }
    if (is_eol) {
        ++have;
    }
    line->set(_line_buffer.get(), have);
    return true;
}

bool GZipFile::IsEof() {
    return _file->IsEof() && _zlib_inflate_stream->avail_in == 0;
}

void GZipFile::GZipRelease() {
    if (_zlib_deflate_stream) {
        _zlib_deflate_stream->next_in = NULL;
        _zlib_deflate_stream->avail_in = 0u;

        int ret = 0;
        do {
            _zlib_deflate_stream->next_out = reinterpret_cast<Bytef*>(_deflat_buffer.get());
            _zlib_deflate_stream->avail_out = _deflat_buffer_size;
            ret = deflate(_zlib_deflate_stream, Z_FINISH);
            CHECK_NE(ret, Z_STREAM_ERROR);
            int have = _deflat_buffer_size - _zlib_deflate_stream->avail_out;
            _file->Write(_deflat_buffer.get(), have);

        } while (_zlib_deflate_stream->avail_out == 0u);

        CHECK_EQ(ret, Z_STREAM_END);

        ret = deflateEnd(_zlib_deflate_stream);
        CHECK_NE(ret, Z_STREAM_ERROR) << "deflateEnd failed!";
        free(_zlib_deflate_stream);
        _zlib_deflate_stream = NULL;
    }
    if (_zlib_inflate_stream) {
        int ret = inflateEnd(_zlib_inflate_stream);
        CHECK_NE(ret, Z_STREAM_ERROR) << "deflateEnd failed!";
        free(_zlib_inflate_stream);
        _zlib_inflate_stream = NULL;
    }
}

void GZipFile::EnlargeBuffer() {
    CHECK_GT(_deflat_buffer_size, 0u);
    if (_deflat_buffer_size >= MAX_BUFFER_SIZE) {
        return;
    }

    size_t original_size = _deflat_buffer_size;

    _deflat_buffer_size *= 2;
    if (_deflat_buffer_size >= MAX_BUFFER_SIZE) {
        _deflat_buffer_size = MAX_BUFFER_SIZE;
    }

    LOG(INFO) << "Enlarge gzip compression buffer from " << original_size
        << " to " << _deflat_buffer_size;

    _deflat_buffer.reset(new char[_deflat_buffer_size]);
}

void GZipFile::ResizeLineBuffer(size_t size) {
    _line_buffer_size = size;
    _line_buffer.reset(new char[_line_buffer_size]);
}

void GZipFile::BufferedReadLine(size_t max_size, size_t* have, bool* is_eol) {
    *is_eol = false;
    if (max_size == 0u || IsEof()) {
        *have = 0u;
        return;
    }
    if (max_size > _line_buffer_size) {
        ResizeLineBuffer(max_size);
    }
    char* line_buffer = _line_buffer.get();
    size_t line_buffer_offset = 0u;
    while (line_buffer_offset < max_size) {
        if (Read(line_buffer, 1) == 0u && IsEof()) {
            // EOF
            break;
        }
        if (*line_buffer == '\n') {
            *is_eol = true;
            break;
        }
        ++line_buffer;
        ++line_buffer_offset;
    }
    *have = line_buffer_offset;
}


COMPRESSION_FILE_FACTORY_REGISTER(GZipFile, "gz");

} // namespace runtime
} // namespace flume
} // namespace baidu

