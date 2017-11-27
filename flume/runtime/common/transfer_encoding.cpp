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

#include "flume/runtime/common/transfer_encoding.h"

#include <netinet/in.h>
#include <cstring>
#include <string>

#include "glog/logging.h"
#include "toft/base/string/string_piece.h"

#include "flume/core/partitioner.h"
#include "flume/runtime/util/serialize_buffer.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace {

class FixSizeField : public TransferEncoding::Field {
public:
    explicit FixSizeField(size_t size) : _size(size) {}

    virtual size_t encode(const toft::StringPiece& key, char* buffer, size_t buffer_size) {
        CHECK_EQ(key.size(), _size);

        if (_size <= buffer_size) {
            std::memcpy(buffer, key.data(), _size);
        }
        return _size;
    }

    virtual size_t decode(const char* buffer, size_t buffer_size,
                          std::vector<toft::StringPiece>* keys) {
        keys->push_back(toft::StringPiece(buffer, _size));
        return _size;
    }

private:
    const size_t _size;
};

class EscapedField : public TransferEncoding::Field {
public:
    static const char kEnd = 0;
    static const char kEscape = 1;

    EscapedField() : _buffer(32 * 1024 /* 32k */) {}

    virtual size_t encode(const toft::StringPiece& key, char* buffer, size_t buffer_size) {
        if (buffer_size <= key.size() * 2) {
            return key.size() * 2 + 1;  // return max needed bytes
        }

        char* ptr = buffer;
        for (size_t i = 0; i < key.size(); ++i) {
            if (key[i] == kEnd || key[i] == kEscape) {
                *ptr++ = kEscape;
            }
            *ptr++ = key[i];
        }
        *ptr++ = kEnd;

        return ptr - buffer;
    }

    virtual size_t decode(const char* buffer, size_t buffer_size,
                          std::vector<toft::StringPiece>* keys) {
        _buffer_guard = _buffer.Reserve(buffer_size);
        char* decoded_data = _buffer_guard.data();
        uint32_t decoded_size = 0;

        const char* ptr = buffer;
        const char* const end = buffer + buffer_size;
        while (ptr < end && *ptr != kEnd) {
            if (*ptr == kEscape) {
                ++ptr;
            }
            decoded_data[decoded_size++] = *ptr++;
        }
        DCHECK_EQ(*ptr, kEnd) << "buffer must be null-terminated!";
        ++ptr; // skip tailing kEnd

        keys->push_back(toft::StringPiece(decoded_data, decoded_size));
        return ptr - buffer;
    }

private:
    SerializeBuffer _buffer;
    SerializeBuffer::Guard _buffer_guard;
};

class TailingField : public TransferEncoding::Field {
public:
    virtual size_t encode(const toft::StringPiece& key, char* buffer, size_t buffer_size) {
        if (key.size() <= buffer_size) {
            std::memcpy(buffer, key.data(), key.size());
        }
        return key.size();
    }

    virtual size_t decode(const char* buffer, size_t buffer_size,
                          std::vector<toft::StringPiece>* keys) {
        keys->push_back(toft::StringPiece(buffer, buffer_size));
        return buffer_size;
    }
};

class OmittedField : public TransferEncoding::Field {
public:
    OmittedField() {}
    explicit OmittedField(const std::string& default_value) : _default_value(default_value) {}

    virtual size_t encode(const toft::StringPiece& key, char* buffer, size_t buffer_size) {
        return 0;
    }

    virtual size_t decode(const char* buffer, size_t buffer_size,
                          std::vector<toft::StringPiece>* keys) {
        keys->push_back(_default_value);
        return 0;
    }

private:
    const std::string _default_value;
};

class UniqueTag : public TransferEncoding::Tag {
public:
    virtual size_t encode(char* buffer, size_t buffer_size) {
        return 0;
    }

    virtual size_t decode(const char* buffer, size_t buffer_size, uint32_t* tag) {
        *tag = 0;
        return 0;
    }
};

class Uint8Tag : public TransferEncoding::Tag {
public:
    Uint8Tag() : _tag(0) {}
    explicit Uint8Tag(uint8_t tag) : _tag(tag) {}

    virtual size_t encode(char* buffer, size_t buffer_size) {
        if (buffer_size >= 1) {
            *reinterpret_cast<uint8_t*>(buffer) = _tag;
        }
        return 1;
    }

    virtual size_t decode(const char* buffer, size_t buffer_size, uint32_t* tag) {
        DCHECK_GE(buffer_size, 1);
        *tag = *reinterpret_cast<const uint8_t*>(buffer);
        return 1;
    }

private:
    const uint8_t _tag;
};

class Uint16Tag : public TransferEncoding::Tag {
public:
    Uint16Tag() : _tag(0) {}
    explicit Uint16Tag(uint16_t tag) : _tag(tag) {}

    virtual size_t encode(char* buffer, size_t buffer_size) {
        if (buffer_size >= 2) {
            *reinterpret_cast<uint16_t*>(buffer) = htons(_tag);
        }
        return 2;
    }

    virtual size_t decode(const char* buffer, size_t buffer_size, uint32_t* tag) {
        DCHECK_GE(buffer_size, 2);
        *tag = ntohs(*reinterpret_cast<const uint16_t*>(buffer));
        return 2;
    }

private:
    const uint16_t _tag;
};

}  // namespace

TransferEncoder::TransferEncoder(const PbTransferEncoder& message) {
    for (const PbTransferEncoder* encoder = &message; encoder != NULL;
            encoder = encoder->has_successor() ? &encoder->successor() : NULL) {
        Stage stage;

        const PbTransferEncodingField& field = encoder->field();
        switch (field.type()) {
            case PbTransferEncodingField::RECORD: {
                // RECORD did not require encoding to key
                continue;
            }
            case PbTransferEncodingField::FIXED_LENGTH_KEY: {
                stage.field = new FixSizeField(field.length());
                break;
            }
            case PbTransferEncodingField::VARIABLE_LENGTH_KEY: {
                stage.field = new EscapedField();
                break;
            }
            case PbTransferEncodingField::TAILING_KEY: {
                stage.field = new TailingField();
                break;
            }
            default: {
                stage.field = new OmittedField();
            }
        }
        _fields.push_back(stage.field);

        switch (encoder->tag_encoding()) {
            case PbTransferEncoder::UNIQUE: {
                stage.tag = new UniqueTag();
                break;
            }
            case PbTransferEncoder::UINT8: {
                stage.tag = new Uint8Tag(encoder->tag());
                break;
            }
            case PbTransferEncoder::UINT16: {
                stage.tag = new Uint16Tag(encoder->tag());
                break;
            }
            default: {
                LOG(FATAL) << "un-supported tag encoding" << message.DebugString();
            }
        }
        _tags.push_back(stage.tag);

        _stages.push_back(stage);
    };
}

uint32_t TransferEncoder::encode(const std::vector<toft::StringPiece>& keys,
                                 char* buffer, size_t buffer_size) const {
    DCHECK_LE(_stages.size(), keys.size());

    size_t used_size = 0;
    for (size_t i = 0; i < _stages.size(); ++i) {
        const Stage* stage = &_stages[i];

        if (used_size <= buffer_size) {
            used_size += stage->field->encode(keys[i],
                                              buffer + used_size, buffer_size - used_size);
        } else {
            used_size += stage->field->encode(keys[i], NULL, 0);
        }

        // LOG(INFO) << used_size;

        if (used_size <= buffer_size) {
            used_size += stage->tag->encode(buffer + used_size, buffer_size - used_size);
        } else {
            used_size += stage->tag->encode(NULL, 0);
        }

        // LOG(INFO) << used_size;
    }

    return used_size;
}

TransferDecoder::TransferDecoder(const PbTransferDecoder& message,
                                 const std::string& global_key, uint32_t local_partition,
                                 const std::map<std::string, uint32_t>& ports) {
    _root = new_stage(message, global_key, local_partition, ports);
}

TransferDecoder::Stage TransferDecoder::new_stage(const PbTransferDecoder& message,
                                                  const std::string& global_key,
                                                  uint32_t local_partition,
                                                  const std::map<std::string, uint32_t>& ports) {
    Stage stage;

    const PbTransferEncodingField& field = message.field();
    switch (field.type()) {
        case PbTransferEncodingField::RECORD: {
            CHECK_EQ(ports.count(field.identity()), 1);
            stage.port = ports.find(field.identity())->second;
            return stage;
        }
        case PbTransferEncodingField::GLOBAL_KEY: {
            stage.field = new OmittedField(global_key);
            break;
        }
        case PbTransferEncodingField::LOCAL_PARTITION: {
            stage.field = new OmittedField(core::EncodePartition(local_partition));
            break;
        }
        case PbTransferEncodingField::FIXED_LENGTH_KEY: {
            stage.field = new FixSizeField(field.length());
            break;
        }
        case PbTransferEncodingField::VARIABLE_LENGTH_KEY: {
            stage.field = new EscapedField();
            break;
        }
        case PbTransferEncodingField::TAILING_KEY: {
            stage.field = new TailingField();
            break;
        }
        default: {
            LOG(FATAL) << "un-supported encoding field: " << message.DebugString();
        }
    }
    _fields.push_back(stage.field);

    CHECK_NE(message.successor_size(), 0);
    if (message.successor_size() == 1) {
        stage.tag = new UniqueTag();
    } else if (message.successor_size() <= 256) {
        stage.tag = new Uint8Tag();
    } else {
        stage.tag = new Uint16Tag();
    }
    _tags.push_back(stage.tag);

    for (int i = 0; i < message.successor_size(); ++i) {
        stage.successors.push_back(
                new_stage(message.successor(i), global_key, local_partition, ports)
        );
    }

    return stage;
}

void TransferDecoder::decode(const char* buffer, size_t buffer_size,
                             uint32_t* port, std::vector<toft::StringPiece>* keys) const {
    const char* const buffer_end = buffer + buffer_size;

    const Stage* stage = &_root;
    while (!stage->successors.empty()) {
        uint32_t tag = 0;
        buffer += stage->field->decode(buffer, buffer_end - buffer, keys);
        buffer += stage->tag->decode(buffer, buffer_end - buffer, &tag);
        stage = &stage->successors[tag];
    }

    *port = stage->port;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
