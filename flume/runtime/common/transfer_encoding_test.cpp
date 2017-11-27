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
// Author: Wen Xiang <wenxiang@baidu.com>
//

#include "flume/runtime/common/transfer_encoding.h"

#include <list>
#include <map>

#include "boost/assign.hpp"
#include "boost/foreach.hpp"
#include "boost/ptr_container/ptr_map.hpp"
#include "boost/scoped_ptr.hpp"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/crypto/uuid/uuid.h"

#include "flume/core/partitioner.h"

namespace baidu {
namespace flume {
namespace runtime {

using namespace boost::assign;

using ::testing::_;
using ::testing::InSequence;
using ::testing::ElementsAre;

class TransferEncodingTest : public ::testing::Test {
public:
    typedef std::list<std::string> KeyList;
    typedef std::pair<std::string, uint32_t> Record;

    class Stage {
    public:
        explicit Stage() : _predecessor(NULL) {
            _field.set_identity("");
            _field.set_type(PbTransferEncodingField::GLOBAL_KEY);
        }

        Stage(const PbTransferEncodingField& field, Stage* predecessor) :
                _field(field), _predecessor(predecessor) {}

        std::string identity() { return _field.identity(); }

        PbTransferEncodingField::Type type() { return _field.type(); }

        Stage* predecessor() { return _predecessor; }

        Stage* add_record() {
            PbTransferEncodingField field;
            field.set_type(PbTransferEncodingField::RECORD);
            return add_successor(field);
        }

        Stage* add_local_partition() {
            PbTransferEncodingField field;
            field.set_type(PbTransferEncodingField::LOCAL_PARTITION);
            return add_successor(field);
        }

        Stage* add_fixed_length_key(uint32_t length) {
            PbTransferEncodingField field;
            field.set_type(PbTransferEncodingField::FIXED_LENGTH_KEY);
            field.set_length(length);
            return add_successor(field);
        }

        Stage* add_variable_length_key() {
            PbTransferEncodingField field;
            field.set_type(PbTransferEncodingField::VARIABLE_LENGTH_KEY);
            return add_successor(field);
        }

        Stage* add_tailing_key() {
            PbTransferEncodingField field;
            field.set_type(PbTransferEncodingField::TAILING_KEY);
            return add_successor(field);
        }

        PbTransferEncoder encoder() {
            PbTransferEncoder message;
            *message.mutable_field() = _field;
            message.set_tag(0);
            message.set_tag_encoding(PbTransferEncoder::UNIQUE);
            return _predecessor->encoder(this, message);
        }

        PbTransferEncoder encoder(Stage* child, PbTransferEncoder successor) {
            PbTransferEncoder message;
            *message.mutable_field() = _field;
            *message.mutable_successor() = successor;

            for (size_t i = 0; i < _successors.size(); ++i) {
                if (&_successors[i] == child) {
                    message.set_tag(i);
                }
            }

            if (_successors.size() <= 1) {
                message.set_tag_encoding(PbTransferEncoder::UNIQUE);
            } else if (_successors.size() <= 256) {
                message.set_tag_encoding(PbTransferEncoder::UINT8);
            } else {
                message.set_tag_encoding(PbTransferEncoder::UINT16);
            }

            if (_predecessor == NULL) {
                return message;
            } else {
                return _predecessor->encoder(this, message);
            }
        }

        PbTransferDecoder decoder() {
            PbTransferDecoder message;
            *message.mutable_field() = _field;
            if (_field.type() != PbTransferEncodingField::RECORD) {
                for (size_t i = 0; i < _successors.size(); ++i) {
                    *message.add_successor() = _successors[i].decoder();
                }
            }
            return message;
        }

    private:
        Stage* add_successor(PbTransferEncodingField field) {
            field.set_identity(toft::CreateCanonicalUUIDString());

            Stage* successor = new Stage(field, this);
            _successors.push_back(successor);
            return successor;
        }

        PbTransferEncodingField _field;
        Stage* _predecessor;
        boost::ptr_vector<Stage> _successors;
    };

    class Writer {
    public:
        Writer() : _stage(NULL), _records(NULL), _buffer_size(0) {}

        Writer(Stage* stage, std::multimap<std::string, uint32_t>* records) :
                _stage(stage), _records(records), _buffer(new char[1]), _buffer_size(1) {}

        uint32_t write(const KeyList& record, uint32_t sequence) {
            if (_encoder.get() == NULL) {
                _encoder.reset(new TransferEncoder(_stage->encoder()));
            }

            std::vector<toft::StringPiece> keys;
            BOOST_FOREACH(const std::string& key, record) {
                keys.push_back(key);
            }

            uint32_t encoded_bytes = _encoder->encode(keys, _buffer.get(), _buffer_size);
            if (encoded_bytes > _buffer_size) {
                _buffer_size = encoded_bytes;
                _buffer.reset(new char[_buffer_size]);

                encoded_bytes = _encoder->encode(keys, _buffer.get(), _buffer_size);
            }
            _records->insert(std::make_pair(std::string(_buffer.get(), encoded_bytes), sequence));

            return encoded_bytes;
        }

    private:
        Stage* _stage;
        std::multimap<std::string, uint32_t>* _records;

        boost::scoped_ptr<TransferEncoder> _encoder;
        boost::scoped_array<char> _buffer;
        uint32_t _buffer_size;
    };

    Stage* root() { return &_root; }

    void assign_port(Stage* stage, uint32_t port) {
        _writers.insert(port, new Writer(stage, &_records));

        while (stage->type() != PbTransferEncodingField::RECORD) {
            stage = stage->predecessor();
        }
        _ports[stage->identity()] = port;
    }

    uint32_t encode(uint32_t port, const KeyList& keys, uint32_t sequence = 0) {
        return _writers[port].write(keys, sequence);
    }

    void decode_records(const std::string& global_key, uint32_t local_partition) {
        boost::scoped_ptr<TransferDecoder> decoder(
                new TransferDecoder(_root.decoder(), global_key, local_partition, _ports)
        );

        BOOST_FOREACH(const Record& record, _records) {
            uint32_t port = 0;
            std::vector<toft::StringPiece> keys;
            decoder->decode(record.first.data(), record.first.size(), &port, &keys);

            KeyList list;
            for (size_t i = 0; i < keys.size(); ++i) {
                list.push_back(keys[i].as_string());
            }
            decode(port, list, record.second);
        }
    }

    MOCK_METHOD3(decode, void(uint32_t, const KeyList&, uint32_t));

private:
    Stage _root;
    std::map<std::string, uint32_t> _ports;
    boost::ptr_map<uint32_t, Writer> _writers;

    std::multimap<std::string, uint32_t> _records;
};

TEST_F(TransferEncodingTest, SingleStreamAtGlobalScope) {
    Stage* one = root()->add_record();

    assign_port(one, 7 /* avoid 0 (which is nature) for test */);
    EXPECT_EQ(0, encode(7, list_of("map-0")));
    EXPECT_EQ(0, encode(7, list_of("map-1")("extra-key")));
    EXPECT_EQ(0, encode(7, list_of("map-2")));

    {
        InSequence in_sequence;

        EXPECT_CALL(*this, decode(7, ElementsAre("reduce"), _));
        EXPECT_CALL(*this, decode(7, ElementsAre("reduce"), _));
        EXPECT_CALL(*this, decode(7, ElementsAre("reduce"), _));
    }
    decode_records("reduce", 0);
}

TEST_F(TransferEncodingTest, OneLevelGroupBy) {
    Stage* group = root()->add_tailing_key();
    Stage* record = group->add_record();

    assign_port(record, 0);
    EXPECT_EQ(6, encode(0, list_of("map-0")("banana")));
    EXPECT_EQ(5, encode(0, list_of("map-1")("apple")));
    EXPECT_EQ(6, encode(0, list_of("map-2")("cherry")("extra-key")));

    {
        InSequence in_sequence;

        EXPECT_CALL(*this, decode(0, ElementsAre("reduce", "apple"), _));
        EXPECT_CALL(*this, decode(0, ElementsAre("reduce", "banana"), _));
        EXPECT_CALL(*this, decode(0, ElementsAre("reduce", "cherry"), _));
    }
    decode_records("reduce", 0);
}

TEST_F(TransferEncodingTest, OneLevelJoin) {
    Stage* group = root()->add_variable_length_key();
    Stage* record_0 = group->add_record();
    Stage* record_1 = group->add_record();

    assign_port(record_0, 0);
    assign_port(record_1, 1);

    EXPECT_EQ(5, encode(0, list_of<std::string>("")(list_of(2)(0))));
    EXPECT_EQ(3, encode(1, list_of<std::string>("")(list_of(2))));
    EXPECT_EQ(4, encode(1, list_of<std::string>("")(list_of(1))));
    EXPECT_EQ(6, encode(1, list_of<std::string>("")(list_of(1)(0))));
    EXPECT_EQ(2, encode(0, list_of<std::string>("")("")));
    EXPECT_EQ(6, encode(0, list_of<std::string>("")(list_of(0)(1))));
    EXPECT_EQ(6, encode(1, list_of<std::string>("")(list_of(0)(1))));
    EXPECT_EQ(4, encode(0, list_of<std::string>("")(list_of(0))));
    EXPECT_EQ(5, encode(1, list_of<std::string>("")(list_of(1)(2))));

    {
        InSequence in_sequence;

        EXPECT_CALL(*this, decode(0, ElementsAre(_, ElementsAre()), _));
        EXPECT_CALL(*this, decode(0, ElementsAre(_, ElementsAre(0)), _));
        EXPECT_CALL(*this, decode(0, ElementsAre(_, ElementsAre(0, 1)), _));
        EXPECT_CALL(*this, decode(1, ElementsAre(_, ElementsAre(0, 1)), _));
        EXPECT_CALL(*this, decode(1, ElementsAre(_, ElementsAre(1)), _));
        EXPECT_CALL(*this, decode(1, ElementsAre(_, ElementsAre(1, 0)), _));
        EXPECT_CALL(*this, decode(1, ElementsAre(_, ElementsAre(1, 2)), _));
        EXPECT_CALL(*this, decode(1, ElementsAre(_, ElementsAre(2)), _));
        EXPECT_CALL(*this, decode(0, ElementsAre(_, ElementsAre(2, 0)), _));
    }
    decode_records("", 0);
}

TEST_F(TransferEncodingTest, MassiveLocalPartitions) {
    std::vector<Stage*> stages;
    for (int i = 0; i <= 256; ++i) {
        stages.push_back(root()->add_local_partition()->add_record());
    }

    for (int i = 0; i <= 256; ++i) {
        assign_port(stages[i], i);
    }

    EXPECT_EQ(2, encode(127, list_of<std::string>("map-0")(core::EncodePartition(2))));
    EXPECT_EQ(2, encode(32, list_of<std::string>("map-1")("")));
    EXPECT_EQ(2, encode(3, list_of<std::string>("map-1")(core::EncodePartition(1023))));

    {
        InSequence in_sequence;

        const std::string kLocalPartition = core::EncodePartition(42);

        EXPECT_CALL(*this, decode(3, ElementsAre(_, kLocalPartition), _));
        EXPECT_CALL(*this, decode(32, ElementsAre(_, kLocalPartition), _));
        EXPECT_CALL(*this, decode(127, ElementsAre(_, kLocalPartition), _));
    }
    decode_records("", 42);
}

TEST_F(TransferEncodingTest, OrderedStream) {
    assign_port(root()->add_fixed_length_key(4)->add_record()->add_tailing_key(), 0);

    EXPECT_EQ(5, encode(0, list_of<std::string>("map-0")(core::EncodePartition(7))("0"), 2));
    EXPECT_EQ(5, encode(0, list_of<std::string>("map-3")(core::EncodePartition(4))("1"), 1));
    EXPECT_EQ(4, encode(0, list_of<std::string>("map-2")(core::EncodePartition(4))(""), 0));
    EXPECT_EQ(6, encode(0, list_of<std::string>("map-1")(core::EncodePartition(7))("00"), 3));

    {
        InSequence in_sequence;

        EXPECT_CALL(*this, decode(_, ElementsAre(_, core::EncodePartition(4)), 0));
        EXPECT_CALL(*this, decode(_, ElementsAre(_, core::EncodePartition(4)), 1));
        EXPECT_CALL(*this, decode(_, ElementsAre(_, core::EncodePartition(7)), 2));
        EXPECT_CALL(*this, decode(_, ElementsAre(_, core::EncodePartition(7)), 3));
    }
    decode_records("reduce", 0);
}

TEST_F(TransferEncodingTest, MultiLevel) {
    Stage* group = root()->add_variable_length_key();

    assign_port(group->add_record(), 0);
    assign_port(group->add_tailing_key()->add_record(), 1);
    assign_port(group->add_record(), 2);

    EXPECT_EQ(8, encode(2, list_of<std::string>("map-0")("second")));
    EXPECT_EQ(8, encode(1, list_of<std::string>("map-0")("first")("a")));
    EXPECT_EQ(7, encode(0, list_of<std::string>("map-0")("first")));
    EXPECT_EQ(8, encode(1, list_of<std::string>("map-0")("second")("")));
    EXPECT_EQ(7, encode(2, list_of<std::string>("map-0")("first")));
    EXPECT_EQ(8, encode(1, list_of<std::string>("map-0")("first")("b")));
    EXPECT_EQ(7, encode(0, list_of<std::string>("map-0")("third")));

    {
        InSequence in_sequence;

        EXPECT_CALL(*this, decode(0, ElementsAre(_, "first"), _));
        EXPECT_CALL(*this, decode(1, ElementsAre(_, "first", "a"), _));
        EXPECT_CALL(*this, decode(1, ElementsAre(_, "first", "b"), _));
        EXPECT_CALL(*this, decode(2, ElementsAre(_, "first"), _));
        EXPECT_CALL(*this, decode(1, ElementsAre(_, "second", ""), _));
        EXPECT_CALL(*this, decode(2, ElementsAre(_, "second"), _));
        EXPECT_CALL(*this, decode(0, ElementsAre(_, "third"), _));
    }
    decode_records("reduce", 0);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
