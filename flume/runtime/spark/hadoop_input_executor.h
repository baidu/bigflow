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

#ifndef FLUME_RUNTIME_SPARK_HADOOP_INPUT_EXECUTOR_H_
#define FLUME_RUNTIME_SPARK_HADOOP_INPUT_EXECUTOR_H_

#include <string>
#include <vector>

#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/runtime/common/general_dispatcher.h"
#include "flume/runtime/common/sub_executor_manager.h"
#include "flume/runtime/dataset.h"
#include "flume/runtime/spark/input_executor.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace spark {

class HadoopInputExecutor : public InputExecutor {
public:
    explicit HadoopInputExecutor(const PbSparkTask::PbHadoopInput& message);
    virtual ~HadoopInputExecutor() {}

    void initialize(const PbExecutor& message,
                    const std::vector<Executor*>& childs,
                    DatasetManager* dataset_manager);

    bool process_input(const toft::StringPiece& key, const toft::StringPiece& value);

    void input_done();

protected:
    // Executor Interfaces
    virtual void Setup(const std::map<std::string, Source*>& sources);
    virtual Source* GetSource(const std::string& id, unsigned scope_level);
    virtual void BeginGroup(const toft::StringPiece& key);
    virtual void FinishGroup();

private:
    std::string _split;
    toft::scoped_ptr<GeneralDispatcher> _output;
    SubExecutorManager _sub_executors;
    PbSparkTask::PbHadoopInput _message;
};

}  // namespace spark
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_SPARK_HADOOP_INPUT_EXECUTOR_H_
