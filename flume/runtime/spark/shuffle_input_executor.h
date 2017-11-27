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
// Author: Wang Cong <wangcong09@baidu.com>
//         Zhang Yuncong <zhangyuncong@baidu.com>
//         Pan Yuchang <panyuchang@baidu.com>
//

#ifndef FLUME_RUNTIME_SPARK_SHUFFLE_INPUT_EXECUTOR_H
#define FLUME_RUNTIME_SPARK_SHUFFLE_INPUT_EXECUTOR_H

#include <map>
#include <string>
#include <vector>

#include "boost/scoped_ptr.hpp"
#include "toft/base/scoped_ptr.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/executor_impl.h"
#include "flume/runtime/executor.h"
#include "flume/runtime/spark/shuffle_protocol.h"

namespace baidu {
namespace flume {
namespace runtime {

class TransferDecoder;

namespace spark {

class ShuffleInputExecutor : public Executor {
public:
    ShuffleInputExecutor(const PbSparkTask::PbShuffleInput& message, uint32_t local_partition);
    virtual ~ShuffleInputExecutor();

    void initialize(const PbExecutor& executor,
                    const std::vector<Executor*>& childs, DatasetManager* manager);

    virtual void process_input_key(const toft::StringPiece& key);
    virtual void process_input_value(const toft::StringPiece& value);

    virtual void input_done();

protected:
    virtual void Setup(const std::map<std::string, Source*>& sources);
    virtual Source* GetSource(const std::string& identity, unsigned scope_level);
    virtual void BeginGroup(const toft::StringPiece& key);
    virtual void FinishGroup();

private:
    typedef PbSparkTask::PbShuffleInput::Channel PbChannel;
    struct Channel {
        Dispatcher* dispatcher;
        uint32_t priority;
    };

    PbSparkTask::PbShuffleInput _message;
    uint32_t _local_partition;
    std::map<std::string, uint32_t> _ports;
    boost::scoped_ptr<internal::DispatcherManager> _dispatchers;

    std::vector<Channel> _channels;
    Channel* _current_channel;
    std::string _buffered_key;
    boost::scoped_ptr<TransferDecoder> _decoder;
    std::vector<toft::StringPiece> _decoded_keys;

    bool _must_keep_empty_group;
};

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif // FLUME_RUNTIME_SPARK_SHUFFLE_INPUT_EXECUTOR_H
