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
//
// Register/Package needed resource files. Registered files will be putted into working
// directory at runtime. Resource is needed by Backend class.

#ifndef FLUME_RUNTIME_RESOURCE_H
#define FLUME_RUNTIME_RESOURCE_H

#include <set>
#include <string>
#include <vector>

#include "boost/filesystem.hpp"
#include "boost/ptr_container/ptr_map.hpp"
#include "toft/base/scoped_ptr.h"
#include "toft/base/uncopyable.h"

namespace baidu {
namespace flume {
namespace runtime {

// A class used to register files/libs/jars. Registered files will appear in working
// directory at runtime.
//
// Resource are organized like directory/file, which are called entry/file. Three top level
// entries are reserved for Flume-Framework: lib/javalib/flume. User can add files to
// lib/javalib by AddDynamicLibrary/AddJavaLibrary, while entry 'flume' can be accessed by
// Backend::GetFlumeEntry.
//
// The design of Resource does not support recovery from failure. If any operation fails, whole
// Resource instance will be invalid, any subsequent operations will become meaningless. User
// can check GetStatus() to verify whether this Resource is valid.
class Resource {
    TOFT_DECLARE_UNCOPYABLE(Resource);
    friend class Backend;

public:
    typedef boost::filesystem::path Path;

    enum Status {
        kOk = 0,
        kNameExists = 1,
        kBadPath = 2,
        kInternalError = 3,
    };

    class Entry {
        TOFT_DECLARE_UNCOPYABLE(Entry);
        friend class Resource;

    public:
        std::string name() const { return m_name; }

        // Format like /flume/work/
        std::string relative_path() const;

        // Add file with read/write privileges
        void AddNormalFile(const std::string& name, const std::string& path);
        void AddNormalFileFromBytes(const std::string& name, const char* buffer, size_t length);
        void AddNormalFileByRename(const std::string& name, const std::string& path);

        // Add file with read/exec privileges
        void AddExecutable(const std::string& name, const std::string& path);
        void AddExecutableFromBytes(const std::string& name, const char* buffer, size_t length);

        // Get a sub-entry under current entry. If given name is not existed, a new entry
        // will be created.
        Entry* GetEntry(const std::string& name);

        std::vector<std::string> ListFiles();
        std::vector<Entry*> ListEntries();

    private:
        Entry(const std::string& name, const Path& temp_dir, Resource* base);

        void AddFile(const std::string& name, const Path& src);
        void AddFile(const std::string& name, const Path& src, bool rename);
        void AddFile(const std::string& name, const Path& src,
                     boost::filesystem::perms perm);
        void AddFile(const std::string& name, const Path& src, bool rename,
                     boost::filesystem::perms perm);

        void DumpFile(const std::string& name, const char* buffer, size_t length,
                      boost::filesystem::perms perm);

    private:
        std::string m_name;
        Path m_temp_dir;
        Resource* m_base;

        std::set<std::string> m_files;
        boost::ptr_map<std::string, Entry> m_sub_entries;
    };

public:
    Resource(bool delete_dir_on_exit = true);
    ~Resource();

    Entry* GetRootEntry() { return m_root_entry.get(); }

    Status GetStatus() const { return m_status; }

    // Add file to ./lib/, add to LD_LIBRARY_PATH at runtime.
    void AddDynamicLibrary(const std::string& name, const std::string& path);
    void AddDynamicLibraryFromBytes(const std::string& name, const char* buffer, size_t length);
    std::vector<std::string> ListDynamicLibraries();

    // Add file to ./javalib/, add to CLASSPATH at runtime.
    void AddJavaLibrary(const std::string& name, const std::string& path);
    void AddJavaLibraryFromBytes(const std::string& name, const char* buffer, size_t length);
    std::vector<std::string> ListJavaLibraries();

    // Add file
    void AddPythonLibrary(const std::string& name, const std::string& path);
    void AddPythonLibraryFromBytes(const std::string& name, const char* buffer, size_t length);
    std::vector<std::string> ListPythonLibraries();

    // Add file to arbitrary directory relative path at ./ and keep the permission
    void AddFile(const std::string& name, const std::string& path);
    // Add file to arbitrary directory relative path at ./ with r/w permission
    void AddFileWithExecutePermission(const std::string& name, const std::string& path);
    void AddFileFromBytes(const std::string& name, const char* buffer, size_t length);

    // Return a path points to directory which can represents this Resource. Returned
    // path will be deleted automaticly after the lifetime of this Resource instance.
    // User must call these methods after finishing all operations to this instance. Any
    // violation will cause undefined behavior.
    std::string ViewAsDirectory();

    // Note: these interfaces are only used by spark backend.
    void SetCacheFileList(const std::string& file_list);
    void SetCacheArchiveList(const std::string& archive_list);

    std::string GetCacheFileList();
    std::string GetCacheArchiveList();
private:
    // for internal usage.
    Entry* GetFlumeEntry();

    // TODO(wangcong09) some comments
    Entry* WalkThrough(Entry* base, const Path& path);

private:
    Status m_status;
    Path m_temp_dir;
    toft::scoped_ptr<Entry> m_root_entry;

    Entry* m_lib_entry;
    Entry* m_javalib_entry;
    Entry* m_pythonlib_entry;
    Entry* m_flume_entry;
    std::set<Entry*> m_reserved_entries;
    bool m_delete_dir_on_exit;

    // remote cache file and archive lists which are set up by cluster manager(yarn/mesos)
    // note: resource don't valid the availability of cache file and archive.
    std::string m_cache_files;
    std::string m_cache_archives;
};

}  // namespace runtime
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_RUNTIME_RESOURCE_H
