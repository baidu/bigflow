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
// Author: Wang Cong <bigflow-opensource@baidu.com>
//         Zhang Yuncong <bigflow-opensource@baidu.com>
//         Pan Yuchang <bigflow-opensource@baidu.com>
//

#include "flume/runtime/spark/shuffle_input_executor.h"

#include <algorithm>
#include <iterator>

#include "boost/dynamic_bitset.hpp"
#include "boost/foreach.hpp"
#include "boost/ptr_container/ptr_map.hpp"
#include "boost/typeof/typeof.hpp"
#include "toft/base/string/string_piece.h"

#include "flume/runtime/common/transfer_encoding.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

ShuffleInputExecutor::ShuffleInputExecutor(const PbSparkTask::PbShuffleInput& message,
                                         uint32_t local_partition) :
        _message(message), _local_partition(local_partition) {
    for (int i = 0; i < _message.channel_size(); ++i) {
        _ports[_message.channel(i).port()] = i;
    }
}

ShuffleInputExecutor::~ShuffleInputExecutor() {
    LOG(INFO) << "Desctructing ShuffleInputExecutor";
}

void ShuffleInputExecutor::initialize(const PbExecutor& executor,
                                     const std::vector<Executor*>& childs,
                                     DatasetManager* manager) {
    _dispatchers.reset(new internal::DispatcherManager(executor, manager));
    _must_keep_empty_group
            = _message.has_must_keep_empty_group() && _message.must_keep_empty_group();
}

void ShuffleInputExecutor::Setup(const std::map<std::string, Source*>& sources) {
    for (int i = 0; i < _message.channel_size(); ++i) {
        const PbChannel& config = _message.channel(i);
        _ports[config.port()] = i;

        Channel channel;
        channel.dispatcher = _dispatchers->get(config.identity());
        channel.priority = config.priority();
        _channels.push_back(channel);
    }
}

Source* ShuffleInputExecutor::GetSource(const std::string& identity, unsigned scope_level) {
    return _dispatchers->get(identity)->GetSource(scope_level);
}

void ShuffleInputExecutor::BeginGroup(const toft::StringPiece& key) {
    _decoder.reset(
            new TransferDecoder(_message.decoder(), key.as_string(), _local_partition, _ports)
    );
    _dispatchers->begin_group(key, 0);
}

void ShuffleInputExecutor::FinishGroup() {
    _dispatchers->finish_group();
}

void ShuffleInputExecutor::process_input_key(const toft::StringPiece& key) {
    _buffered_key = std::string(key.data(), key.size());
    int64_t key_size = _buffered_key.size();
    const ShuffleHeader* header = reinterpret_cast<const ShuffleHeader*>(_buffered_key.data());
    DCHECK_GE(_buffered_key.size(), sizeof(*header));
    key_size -= sizeof(*header);

    uint32_t port = 0;
    _decoded_keys.clear();
    _decoder->decode(header->content(), key_size, &port, &_decoded_keys);

    _current_channel = &_channels[port];
    _dispatchers->close_prior_dispatchers(_current_channel->priority);

}

void ShuffleInputExecutor::process_input_value(const toft::StringPiece& value) {
    if (!_must_keep_empty_group) {
        _current_channel->dispatcher->EmitBinary(
                _decoded_keys,
                value);
    } else if (value.size() == 0) {
        // empty group
        _current_channel->dispatcher->EmitBinary(
                _decoded_keys,
                toft::StringPiece());
    } else {
        _current_channel->dispatcher->EmitBinary(
                _decoded_keys,
                toft::StringPiece(value.data(), value.size() - 1));
    }
}

void ShuffleInputExecutor::input_done() {
    _dispatchers->done();
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
