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

#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "flume/runtime/resource.h"

#include "boost/filesystem/fstream.hpp"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "toft/base/scoped_ptr.h"

namespace baidu {
namespace flume {
namespace runtime {

typedef boost::filesystem::path Path;

using ::testing::Contains;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Property;

class ResourceTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        m_resource.reset(new Resource());
        m_temp_dir = m_resource->ViewAsDirectory();
    }

    virtual void TearDown() {
        m_resource.reset();
        ASSERT_TRUE(!boost::filesystem::exists(m_temp_dir));
    }

    std::string RelativePath(const Path& path) {
        // ungly way to get relative path
        std::string base = m_temp_dir.string();
        std::string full = path.string();
        return full.substr(full.find_first_of(base) + base.size());
    }

    std::set<std::string> ListAll() {
        return Walk(boost::filesystem::file_status(boost::filesystem::type_unknown));
    }

    std::set<std::string> ListEntries() {
        return Walk(boost::filesystem::file_status(boost::filesystem::directory_file));
    }

    std::set<std::string> ListNormalFiles() {
        return Walk(boost::filesystem::file_status(boost::filesystem::regular_file,
                                                   boost::filesystem::owner_read |
                                                   boost::filesystem::owner_write |
                                                   boost::filesystem::group_read));
    }

    std::set<std::string> ListExecutableFiles() {
        return Walk(boost::filesystem::file_status(boost::filesystem::regular_file,
                                                   boost::filesystem::owner_read |
                                                   boost::filesystem::owner_exe |
                                                   boost::filesystem::group_read |
                                                   boost::filesystem::group_exe));
    }

    std::set<std::string> Walk(boost::filesystem::file_status filter) {
        std::set<std::string> result;
        boost::filesystem::recursive_directory_iterator ptr(m_temp_dir), end;
        for (; ptr != end; ++ptr) {
            using boost::filesystem::type_unknown;
            if (filter.type() != type_unknown && filter.type() != ptr->status().type()) {
                continue;
            }

            using boost::filesystem::perms_not_known;
            if (filter.permissions() != perms_not_known &&
                    filter.permissions() != ptr->status().permissions()) {
                continue;
            }

            result.insert(RelativePath(ptr->path()));
        }

        return result;
    }

    std::string ReadAll(const Path& path) {
        boost::filesystem::ifstream stream(path);
        return std::string(std::istreambuf_iterator<char>(stream),
                           std::istreambuf_iterator<char>());
    }

    Path Canonical(const std::string& symlink) {
        // return boost::filesystem::read_symlink(Path(symlink)).string();
        return boost::filesystem::canonical(Path(symlink));
    }


protected:
    toft::scoped_ptr<Resource> m_resource;
    Path m_temp_dir;
};

TEST_F(ResourceTest, Empty) {
    const char* all[] = {
        "/flume",
        "/javalib",
        "/lib",
        "/pythonlib",
    };
    EXPECT_THAT(ListAll(), ElementsAreArray(all));
    EXPECT_THAT(ListEntries(), ElementsAreArray(all));
}

TEST_F(ResourceTest, EntryOperation) {
    Resource::Entry* root = m_resource->GetRootEntry();
    ASSERT_TRUE(root != NULL);
    EXPECT_EQ(std::string(), root->name());
    EXPECT_THAT(root->ListFiles(), ElementsAre());

    root->GetEntry("text")->AddNormalFile("file1", "testdata/hamlet.txt");
    EXPECT_EQ("text", root->GetEntry("text")->name());

    char raw_text[] = "my name is ozymandias, king of kings";
    root->GetEntry("text")->AddNormalFileFromBytes("file2", raw_text, sizeof(raw_text));

    EXPECT_THAT(root->GetEntry("text")->ListFiles(), ElementsAre("file1", "file2"));
    EXPECT_THAT(root->GetEntry("text")->ListEntries(), ElementsAre());

    Resource::Entry* usr = root->GetEntry("usr");
    usr->GetEntry("bin")->AddExecutable("script1", "testdata/hello.sh");
    EXPECT_EQ("bin", usr->GetEntry("bin")->name());
    EXPECT_EQ("/usr/bin", usr->GetEntry("bin")->relative_path());

    char raw_script[] = "#!/bin/bash\r exit 0";
    usr->GetEntry("sbin")->AddExecutableFromBytes("script2", raw_script, sizeof(raw_script));
    EXPECT_EQ("sbin", usr->GetEntry("sbin")->name());
    EXPECT_EQ("/usr/sbin", usr->GetEntry("sbin")->relative_path());

    EXPECT_THAT(usr->ListFiles(), ElementsAre());
    EXPECT_THAT(usr->ListEntries(), Contains(Property(&Resource::Entry::name, "bin")));
    EXPECT_THAT(usr->ListEntries(), Contains(Property(&Resource::Entry::name, "sbin")));

    EXPECT_EQ(Resource::kOk, m_resource->GetStatus());

    const char* all[] = {
        "/flume",
        "/javalib",
        "/lib",
        "/pythonlib",
        "/text",
        "/text/file1",
        "/text/file2",
        "/usr",
        "/usr/bin",
        "/usr/bin/script1",
        "/usr/sbin",
        "/usr/sbin/script2",
    };
    EXPECT_THAT(ListAll(), ElementsAreArray(all));
    EXPECT_THAT(ListEntries(), ElementsAre("/flume", "/javalib", "/lib", "/pythonlib",
                                           "/text", "/usr", "/usr/bin", "/usr/sbin"));

    EXPECT_THAT(ListNormalFiles(), ElementsAre("/text/file1", "/text/file2"));
    EXPECT_EQ(ReadAll("testdata/hamlet.txt"), ReadAll(m_temp_dir / "text" / "/file1"));
    EXPECT_EQ(std::string(raw_text, sizeof(raw_text)), ReadAll(m_temp_dir / "text" / "file2"));

    EXPECT_THAT(ListExecutableFiles(), ElementsAre("/usr/bin/script1", "/usr/sbin/script2"));
    EXPECT_EQ(ReadAll("testdata/hello.sh"), ReadAll(m_temp_dir / "usr" / "bin" / "script1"));
    EXPECT_EQ(std::string(raw_script, sizeof(raw_script)),
              ReadAll(m_temp_dir / "usr" / "sbin" / "script2"));
}

TEST_F(ResourceTest, DynamicLibrary) {
    m_resource->AddDynamicLibrary("a.so", "testdata/hello.sh");
    m_resource->AddDynamicLibraryFromBytes("b.so", "xxx", 3);
    EXPECT_THAT(m_resource->ListDynamicLibraries(), ElementsAre("a.so", "b.so"));
    EXPECT_EQ(Resource::kOk, m_resource->GetStatus());

    const char* all[] = {
        "/flume",
        "/javalib",
        "/lib",
        "/lib/a.so",
        "/lib/b.so",
        "/pythonlib",
    };
    EXPECT_THAT(ListAll(), ElementsAreArray(all));
    EXPECT_THAT(ListExecutableFiles(), ElementsAre("/lib/a.so", "/lib/b.so"));
    EXPECT_EQ(ReadAll("testdata/hello.sh"), ReadAll(m_temp_dir / "lib" / "a.so"));
    EXPECT_EQ("xxx", ReadAll(m_temp_dir / "lib" / "b.so"));
}

TEST_F(ResourceTest, JavaLibrary) {
    m_resource->AddJavaLibrary("a.jar", "testdata/hello.sh");
    m_resource->AddJavaLibraryFromBytes("b.jar", "xxx", 3);
    EXPECT_THAT(m_resource->ListJavaLibraries(), ElementsAre("a.jar", "b.jar"));
    EXPECT_EQ(Resource::kOk, m_resource->GetStatus());

    const char* all[] = {
        "/flume",
        "/javalib",
        "/javalib/a.jar",
        "/javalib/b.jar",
        "/lib",
        "/pythonlib",
    };
    EXPECT_THAT(ListAll(), ElementsAreArray(all));
    EXPECT_THAT(ListNormalFiles(), ElementsAre("/javalib/a.jar", "/javalib/b.jar"));
    EXPECT_EQ(ReadAll("testdata/hello.sh"), ReadAll(m_temp_dir / "javalib" / "a.jar"));
    EXPECT_EQ("xxx", ReadAll(m_temp_dir / "javalib" / "b.jar"));
}

TEST_F(ResourceTest, PythonLibrary) {
    m_resource->AddPythonLibrary("a.egg", "testdata/hello.sh");
    m_resource->AddPythonLibraryFromBytes("b.egg", "xxx", 3);
    EXPECT_THAT(m_resource->ListPythonLibraries(), ElementsAre("a.egg", "b.egg"));
    EXPECT_EQ(Resource::kOk, m_resource->GetStatus());

    const char* all[] = {
        "/flume",
        "/javalib",
        "/lib",
        "/pythonlib",
        "/pythonlib/a.egg",
        "/pythonlib/b.egg",
    };
    EXPECT_THAT(ListAll(), ElementsAreArray(all));
    EXPECT_THAT(ListNormalFiles(), ElementsAre("/pythonlib/a.egg", "/pythonlib/b.egg"));
    EXPECT_EQ(ReadAll("testdata/hello.sh"), ReadAll(m_temp_dir / "pythonlib" / "a.egg"));
    EXPECT_EQ("xxx", ReadAll(m_temp_dir / "pythonlib" / "b.egg"));
}

TEST_F(ResourceTest, AddFile) {
    m_resource->AddFile("upb/cc/hello.sh", "testdata/hello.sh");
    m_resource->AddFile("hamlet.sh", "testdata/hamlet.txt");
    EXPECT_EQ(Resource::kOk, m_resource->GetStatus());

    const char* all[] = {
        "/flume",
        "/hamlet.sh",
        "/javalib",
        "/lib",
        "/pythonlib",
        "/upb",
        "/upb/cc",
        "/upb/cc/hello.sh",
    };
    EXPECT_THAT(ListAll(), ElementsAreArray(all));

    std::string base(m_resource->ViewAsDirectory());

    {
        Path p1 = Canonical("testdata/hello.sh");
        Path p2 = Canonical(base + "/upb/cc/hello.sh");
        EXPECT_EQ(
                boost::filesystem::status(p1).permissions(),
                boost::filesystem::status(p2).permissions());
    }

    {
        Path p1 = Canonical("testdata/hamlet.txt");
        Path p2 = Canonical(base + "/hamlet.sh");
        EXPECT_EQ(
                boost::filesystem::status(p1).permissions(),
                boost::filesystem::status(p2).permissions());
    }
}

TEST_F(ResourceTest, AddFileWithExecutePermission) {
    m_resource->AddFileWithExecutePermission("upb/cc/hello.sh", "testdata/hello.sh");
    m_resource->AddFileWithExecutePermission("hamlet.sh", "testdata/hamlet.txt");
    EXPECT_EQ(Resource::kOk, m_resource->GetStatus());

    std::string base(m_resource->ViewAsDirectory());
    {
        Path p1 = Canonical("testdata/hello.sh");
        Path p2 = Canonical(base + "/upb/cc/hello.sh");
        EXPECT_EQ(
                boost::filesystem::status(p1).permissions() |
                    boost::filesystem::owner_exe | boost::filesystem::group_exe,
                boost::filesystem::status(p2).permissions());
    }

    {
        Path p1 = Canonical("testdata/hamlet.txt");
        Path p2 = Canonical(base + "/hamlet.sh");
        EXPECT_EQ(
                boost::filesystem::status(p1).permissions() |
                    boost::filesystem::owner_exe | boost::filesystem::group_exe,
                boost::filesystem::status(p2).permissions());
    }
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
