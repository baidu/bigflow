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
// Author: Wen Xiang <wenxiang@baidu.com>

#include "flume/runtime/backend.h"

#include <cstdlib>
#include <queue>

#include "boost/foreach.hpp"
#include "boost/make_shared.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "toft/base/scoped_ptr.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/storage/path/path.h"
#include "toft/storage/seqfile/local_sequence_file_writer.h"
#include "toft/storage/seqfile/local_sequence_file_reader.h"

#include "flume/core/entity.h"
#include "flume/core/loader.h"
#include "flume/core/sinker.h"
#include "flume/runtime/common/file_cache_manager.h"

DEFINE_bool(flume_execute, false, "given if flume binary run in execution mode");
DEFINE_string(flume_backend, "", "which backend to use");
DEFINE_bool(flume_commit, false, "given if flume backend does custom commit");

namespace baidu {
namespace flume {
namespace runtime {

using core::Entity;
using core::Loader;
using core::Sinker;
using core::Committer;
using core::Objector;

Backend* Backend::s_worker_instance = NULL;

void Backend::RunAsWorker() {
    // We do not allow RunAsWorker to be called concurrently.
    CHECK(s_worker_instance == NULL);

    s_worker_instance = CREATE_BACKEND(FLAGS_flume_backend);
    s_worker_instance->Execute();

    delete s_worker_instance;
    s_worker_instance = NULL;
}

const Session& Backend::GetSession() const {
    return *m_session.get();
}

void Backend::AbortCurrentJob(const std::string& reason, const std::string& detail) {
    CHECK_NOTNULL(s_worker_instance);
    s_worker_instance->Abort(reason, detail);
}

Resource::Entry* Backend::GetFlumeEntry(Resource* resource) {
    return resource->GetFlumeEntry();
}

void Backend::GetExecutors(const PbExecutor& root,
                           const PbExecutor_Type& type,
                           std::vector<PbExecutor>* executors) {
    std::queue<PbExecutor> executor_queue;
    executor_queue.push(root);
    while (!executor_queue.empty()) {
        PbExecutor executor = executor_queue.front();
        for (int i = 0; i < executor.child_size(); ++i) {
            executor_queue.push(executor.child(i));
        }
        if (type == executor.type()) {
            executors->push_back(executor);
        }
        executor_queue.pop();
    }
}

void Backend::CommitOutputs(const std::vector<PbExecutor>& executors) {
    BOOST_FOREACH(PbExecutor executor, executors) {
        const PbLogicalPlanNode& node = executor.logical_executor().node();
        if (node.type() == PbLogicalPlanNode_Type_SINK_NODE) {
            Entity<Sinker> entity = Entity<Sinker>::From(node.sink_node().sinker());
            toft::scoped_ptr<Sinker> sinker(entity.CreateAndSetup());
            Committer* committer = sinker->GetCommitter();
            if (NULL != committer) {
                committer->Commit();
            }
        }
    }
}

PbScope* Backend::GetScope(const std::string& id, PbLogicalPlan* plan) {
    for (int32_t i = 0; i < plan->scope_size(); ++i) {
        PbScope* scope = plan->mutable_scope(i);
        if (id == scope->id()) {
            return scope;
        }
    }
    return NULL;
}

void Backend::GenerateSplitInfo(
        const std::string& split_path,
        const PbLogicalPlan& src,
        PbLogicalPlan* dst) {
    *dst = src;
    typedef toft::LocalSequenceFileWriter Writer;
    // generate file
    std::string file_path = split_path;
    toft::scoped_ptr<toft::File> file(toft::File::Open(file_path, "w"));
    toft::scoped_ptr<Writer> writer(new Writer(file.release()));
    CHECK(writer->Init());
    for (int32_t i = 0; i < src.node_size(); ++i) {
        uint32_t concurrency = 0;
        const PbLogicalPlanNode& node = src.node(i);
        if (node.type() != PbLogicalPlanNode::LOAD_NODE
                && node.type() != PbLogicalPlanNode::SINK_NODE) {
            continue;
        }
        if (node.type() == PbLogicalPlanNode::LOAD_NODE) {
            Entity<Loader> entity = Entity<Loader>::From(node.load_node().loader());
            toft::scoped_ptr<Loader> loader(entity.CreateAndSetup());
            for (int32_t j = 0; j < node.load_node().uri_size(); ++j) {
                std::vector<std::string> split_info;
                loader->Split(node.load_node().uri(j), &split_info);
                concurrency += split_info.size();
                BOOST_FOREACH(std::string split, split_info) {
                    writer->WriteRecord(node.scope(), split);
                }
            }
        } else {
            Entity<Sinker> entity = Entity<Sinker>::From(node.sink_node().sinker());
            toft::scoped_ptr<Sinker> sinker(entity.CreateAndSetup());
            std::vector<std::string> split_info;
            sinker->Split(&split_info);
            concurrency += split_info.size();
        }
        if (0 != concurrency) {
            std::string scope_id = node.scope();
            PbScope* scope = Backend::GetScope(scope_id, dst);
            CHECK(NULL != scope) << "scope id: " << scope_id;
            CHECK(!Backend::GetScope(scope->father(), dst)->has_father());
            scope->set_concurrency(concurrency);
            if (scope->type() == PbScope::BUCKET) {
                scope->mutable_bucket_scope()->set_bucket_size(concurrency);
            }
        }
    }
    CHECK(writer->Close()) << "Error closing file";
}

void Backend::GetSplitInfo(
        const std::string& split_path,
        std::map<std::string, std::vector<std::string> >* split_info) {
    CHECK_NOTNULL(split_info);
    typedef toft::LocalSequenceFileReader Reader;
    toft::scoped_ptr<toft::File> file(toft::File::Open(split_path, "r"));
    toft::scoped_ptr<Reader> reader(new Reader(file.release()));
    CHECK(reader->Init());
    std::string key;
    std::string value;
    while (reader->ReadRecord(&key, &value)) {
        (*split_info)[key].push_back(value);
    }
}

bool Backend::IsNodeCached(const std::string& identity) const {
    return GetSession().GetCachedNodeIds().count(identity) != 0;
}

CacheManager* Backend::GetCacheManager() {
    if (NULL == m_cache_manager.get()) {
        m_cache_manager.reset(CreateCacheManager());
        CHECK_NOTNULL(m_cache_manager.get());
    }
    return m_cache_manager.get();
}

boost::shared_ptr<KVIterator> Backend::GetCachedData(const std::string& identity) {
    CHECK(IsNodeCached(identity));
    CacheManager::Reader* reader = this->GetCacheManager()->GetReader(identity);
    Session::NodesMap nodes = GetSession().GetNodesMap();
    CHECK_EQ(1u, nodes.count(identity));
    const PbEntity& objector = nodes[identity].objector();
    return boost::make_shared<CacheIterator>(reader, Entity<Objector>::From(objector));
}

Backend::Backend() {
}

Backend::Status Backend::Suspend(const PbLogicalPlan& plan, Resource* resource) {
    return Status(false, "Suspend operation not be supported", "");
}

Backend::Status Backend::Kill(const PbLogicalPlan& plan, Resource* resource) {
    return Status(false, "Kill operation not be supported", "");
}

Backend::Status Backend::GetStatus(
        const PbLogicalPlan& plan,
        Resource* resource,
        Backend::AppStatus* status) {
    return Status(false, "GetStatus operation not be supported", "");
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
