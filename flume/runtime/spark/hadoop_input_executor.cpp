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

#include "flume/runtime/spark/hadoop_input_executor.h"

#include "flume/core/entity.h"
#include "flume/runtime/io/io_format.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

HadoopInputExecutor::HadoopInputExecutor(const PbSparkTask::PbHadoopInput& message):
    _split("SPLIT0"), _message(message) {}

void HadoopInputExecutor::initialize(
        const PbExecutor& message,
        const std::vector<Executor*>& childs,
        DatasetManager* dataset_manager) {
    using core::Entity;
    using core::Objector;

    _sub_executors.Initialize(message, childs);

    _output.reset(new GeneralDispatcher(_message.id(), 1));
    _output->SetDatasetManager(dataset_manager);
    _output->SetObjector(Entity<Objector>::Of<RecordObjector>("").ToProtoMessage());
}

bool HadoopInputExecutor::process_input(
        const toft::StringPiece& key,
        const toft::StringPiece& value) {
    Record record(key, value);
    return _output->EmitObject(&record);
}

void HadoopInputExecutor::input_done() {
    // done scope-1
    _output->Done();

    // finish scope-0
    _output->FinishGroup();
    _sub_executors.FinishGroup();
}

void HadoopInputExecutor::Setup(const std::map<std::string, Source*>& sources) {
    std::map<std::string, Source*> inputs = sources;
    inputs[_message.id()] = _output->GetSource(1);
    _sub_executors.Setup(inputs);
}

Source* HadoopInputExecutor::GetSource(const std::string& id, unsigned scope_level) {
    if (_message.id() == id) {
        return _output->GetSource(scope_level);
    } else {
        return _sub_executors.GetSource(id, scope_level);
    }
}

void HadoopInputExecutor::BeginGroup(const toft::StringPiece& key) {
    _output->BeginGroup(key);
    _sub_executors.BeginGroup(key);
}

void HadoopInputExecutor::FinishGroup() {
    // begin scope-1
    _output->BeginGroup(_split);
    _sub_executors.BeginGroup(_split);
    _sub_executors.FinishGroup();
    _output->FinishGroup();
}

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu
