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
// Author: Wen Xiang <bigflow-opensource@baidu.com>
//

#ifndef FLUME_RUNTIME_COMMON_TRANSFER_ENCODING_H_
#define FLUME_RUNTIME_COMMON_TRANSFER_ENCODING_H_

#include <stdint.h>

#include <map>
#include <string>
#include <vector>

#include "boost/ptr_container/ptr_vector.hpp"
#include "toft/base/string/string_piece.h"

#include "flume/proto/transfer_encoding.pb.h"

namespace baidu {
namespace flume {
namespace runtime {

class TransferEncoding {
public:
    class Field {
    public:
        virtual ~Field() {}

        virtual size_t encode(const toft::StringPiece& key,
                              char* buffer, size_t buffer_size) = 0;

        virtual size_t decode(const char* buffer, size_t buffer_size,
                              std::vector<toft::StringPiece>* keys) = 0;
    };

    class Tag {
    public:
        virtual ~Tag() {}

        virtual size_t encode(char* buffer, size_t buffer_size) = 0;

        virtual size_t decode(const char* buffer, size_t buffer_size, uint32_t* tag) = 0;
    };

protected:
    boost::ptr_vector<Field> _fields;
    boost::ptr_vector<Tag> _tags;
};

class TransferEncoder : public TransferEncoding {
public:
    explicit TransferEncoder(const PbTransferEncoder& message);

    // return encoded key
    uint32_t encode(const std::vector<toft::StringPiece>& keys,
                    char* buffer, size_t buffer_size) const;

private:
    struct Stage {
        Field* field;
        Tag* tag;
    };

    std::vector<Stage> _stages;
};

class TransferDecoder : public TransferEncoding {
public:
    TransferDecoder(const PbTransferDecoder& message,
                    const std::string& global_key, uint32_t local_partition,
                    const std::map<std::string, uint32_t>& ports);

    void decode(const char* buffer, size_t buffer_size,
                uint32_t* port, std::vector<toft::StringPiece>* keys) const;

private:
    struct Stage {
        Field* field;
        Tag* tag;
        std::vector<Stage> successors;
        uint32_t port;  // set at bottom stage, where has no successors

        Stage() : field(NULL), tag(NULL), port(-1) {}
    };

    Stage new_stage(const PbTransferDecoder& message,
                    const std::string& global_key, uint32_t local_partition,
                    const std::map<std::string, uint32_t>& ports);

    Stage _root;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_COMMON_TRANSFER_ENCODING_H_
