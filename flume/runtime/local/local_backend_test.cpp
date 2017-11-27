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
// Author: Wen Xiang <bigflow-opensource@baidu.com>

#include "flume/runtime/local/local_backend.h"

#include <cstring>
#include <string>
#include <vector>

#include "boost/filesystem.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "toft/base/class_registry.h"
#include "toft/storage/file/file.h"

#include "flume/core/loader.h"
#include "flume/core/logical_plan.h"
#include "flume/core/objector.h"
#include "flume/core/sinker.h"
#include "flume/flume.h"
#include "flume/runtime/backend.h"
#include "flume/runtime/counter.h"
#include "flume/runtime/resource.h"

namespace baidu {
namespace flume {
namespace runtime {
namespace local {

using core::Loader;
using core::LogicalPlan;
using core::Sinker;
using core::Objector;

DEFINE_COUNTER(file_number);

class TestLoader : public core::Loader {
public:
    virtual void Setup(const std::string& config) {
    }

    virtual void Split(const std::string& uri, std::vector<std::string>* splits) {
        ++COUNTER_file_number;
        splits->push_back(uri);
    }

    virtual void Load(const std::string& split, core::Emitter* emitter) {
        std::string content;
        if (toft::File::ReadAll(split, &content)) {
            emitter->Emit(&content);
        }
    }
};

class TestSinker : public core::Sinker {
public:
    virtual void Setup(const std::string& config) {
        m_path = config;
    }

    virtual void Open(const std::vector<toft::StringPiece>& keys) {
        m_output.reset(toft::File::Open(m_path, "w+"));
    }

    virtual void Sink(void* object) {
        std::string* str = static_cast<std::string*>(object);
        m_output->Write(str->data(), str->size());
    }

    virtual void Close() {
        m_output.reset();
    }

private:
    std::string m_path;
    toft::scoped_ptr<toft::File> m_output;
};

class TestObjector : public core::Objector {
public:
    virtual void Setup(const std::string& config) {
    }

    virtual uint32_t Serialize(void* object, char* buffer, uint32_t buffer_size) {
        std::string* str = static_cast<std::string*>(object);
        if (str->size() <= buffer_size) {
            std::memcpy(buffer, str->data(), str->size());
        }
        return str->size();
    }

    virtual void* Deserialize(const char* buffer, uint32_t buffer_size) {
        std::string* str = new std::string(buffer, buffer_size);
        return str;
    }

    virtual void Release(void* object) {
        std::string* str = static_cast<std::string*>(object);
        delete str;
    }
};

std::string GetAbsolutePath(const std::string& name) {
    typedef boost::filesystem::path Path;
    Path path = boost::filesystem::current_path() / name;
    return path.string();
}

TEST(LocalBackendTest, Test) {
    toft::scoped_ptr<LogicalPlan> plan(new LogicalPlan());
    plan->Load(GetAbsolutePath("testdata/hamlet.txt"))->By<TestLoader>()->As<TestObjector>()
        ->SinkBy<TestSinker>(GetAbsolutePath("testdata/output.txt"));

    LocalBackend backend;
    ASSERT_TRUE(plan->Run(&backend, new Resource()));

    std::string origin, output;
    ASSERT_EQ(true, toft::File::ReadAll("testdata/hamlet.txt", &origin));
    ASSERT_EQ(true, toft::File::ReadAll("testdata/output.txt", &output));
    ASSERT_EQ(origin, output);
}

}  // namespace local
}  // namespace runtime
}  // namespace flume
}  // namespace baidu

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::baidu::flume::InitBaiduFlume();
    return RUN_ALL_TESTS();
}
