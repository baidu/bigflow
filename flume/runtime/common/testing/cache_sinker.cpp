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
// Author: Zhang Yuncong <bigflow-opensource@baidu.com>
//
// cache sinker

#include "flume/runtime/common/testing/cache_sinker.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "toft/base/scoped_ptr.h"
#include "toft/base/string/string_piece.h"
#include "toft/storage/seqfile/local_sequence_file_writer.h"

#include "flume/proto/physical_plan.pb.h"
#include "flume/util/path_util.h"

namespace baidu {
namespace flume {
namespace runtime {

namespace {

class CacheSinkerImpl : public CacheSinker {
public:
    virtual ~CacheSinkerImpl();

    // the config should be the output uri with toft style
    virtual void Setup(const std::string& config);

    virtual void Open(const std::vector<toft::StringPiece>& keys);

    virtual void Sink(void* object);

    virtual void Close();

private:
    toft::scoped_ptr<toft::LocalSequenceFileWriter> m_writer;
};

CacheSinkerImpl::~CacheSinkerImpl() {
    CHECK_NOTNULL(m_writer.get());
    m_writer->Close();
    m_writer.reset();
}

// config is source_node_id
void CacheSinkerImpl::Setup(const std::string& config) {
    // if there is no this task, it must be in local pipeline
    std::string partition = google::StringFromEnv("mapred_task_partition", "0");
    std::string file_name = config;

    if (!util::IsHdfsPath(file_name)) {
        std::string cmd = "mkdir -p ";
        cmd += file_name;
        CHECK_EQ(0, system(cmd.c_str()));
    }
    file_name += "/" + partition;
    toft::scoped_ptr<toft::File> file(toft::File::Open(file_name, "w"));
    m_writer.reset(new toft::LocalSequenceFileWriter(file.release()));
    m_writer->Init();
}

void CacheSinkerImpl::Open(const std::vector<toft::StringPiece>& keys) {
    // every open first key start with "s"
    // continue keys key start with "k"
    std::string key = "s";
    for(size_t i = 0; i != keys.size(); ++i) {
        m_writer->WriteRecord(key, keys[i]);
        key = "k";
    }
}

void CacheSinkerImpl::Sink(void* object) {
    CHECK_NOTNULL(m_writer.get());
    const toft::StringPiece& input = *static_cast<toft::StringPiece*>(object);
    m_writer->WriteRecord("", input);
}

void CacheSinkerImpl::Close() { }

} // namespace

void CacheSinker::Setup(const std::string& config) {
    m_impl.reset(new CacheSinkerImpl());
    CHECK_NOTNULL(m_impl.get());
    m_impl->Setup(config);
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
