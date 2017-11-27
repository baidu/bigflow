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

#include <stdlib.h>

#include <memory>
#include <string>
#include <vector>

#include "flume/runtime/resource.h"

#include "boost/filesystem/fstream.hpp"
#include "glog/logging.h"
#include "toft/crypto/uuid/uuid.h"
#include "toft/storage/path/path.h"

namespace baidu {
namespace flume {
namespace runtime {

Resource::Resource(bool delete_dir_on_exit) : m_status(kOk) {
    std::string temp_name = ".flume-resource-" + toft::CreateCanonicalUUIDString();
    // Use toft::Path::ToAbsolute() instead of boost::filesystem::current_path() to avoid throwing
    // exceptions when current system locale is not supported.
    m_temp_dir = Path(toft::Path::ToAbsolute(temp_name));

    boost::system::error_code ec;
    if (!boost::filesystem::create_directory(m_temp_dir, ec)) {
        m_status = kInternalError;
    }
    m_root_entry.reset(new Entry("", m_temp_dir, this));

    m_lib_entry = m_root_entry->GetEntry("lib");
    m_reserved_entries.insert(m_lib_entry);

    m_javalib_entry = m_root_entry->GetEntry("javalib");
    m_reserved_entries.insert(m_javalib_entry);

    m_pythonlib_entry = m_root_entry->GetEntry("pythonlib");
    m_reserved_entries.insert(m_pythonlib_entry);

    m_flume_entry = m_root_entry->GetEntry("flume");
    m_reserved_entries.insert(m_flume_entry);

    m_delete_dir_on_exit = delete_dir_on_exit;
}

Resource::~Resource() {
    m_root_entry.reset();
    if (m_delete_dir_on_exit) {
        boost::system::error_code ec;
        boost::filesystem::remove_all(m_temp_dir, ec);
        if (ec) {
            LOG(WARNING) << "Fail to clean temporary files (" << m_temp_dir << "): " << ec;
        }
    }
}

Resource::Entry::Entry(const std::string& name, const Path& temp_dir, Resource* base)
        : m_name(name), m_temp_dir(temp_dir), m_base(base)  {}

void Resource::Entry::AddFile(const std::string& name, const Path& src, bool rename) {
    if (m_files.count(name) || m_sub_entries.count(name)) {
        m_base->m_status = kNameExists;
        LOG(WARNING) << "Trying to add duplicated file: " << (m_temp_dir / name);
        return;
    }

    if (!boost::filesystem::is_regular_file(src) && !boost::filesystem::is_symlink(src)) {
        LOG(WARNING) << "[" << src << "] is neither a regular_file nor a symlink.";
        m_base->m_status = kBadPath;
        return;
    }
    Path dst = m_temp_dir / name;  // concat path

    boost::system::error_code ec;
    if (rename) {
        boost::filesystem::rename(src, dst, ec);
    } else {
        boost::filesystem::copy_file(src, dst, ec);
    }
    if (ec) {
        LOG(WARNING) << "Fail to add file (" << src << "): " << ec;
        m_base->m_status = kInternalError;
        return;
    }
    m_files.insert(name);
}

void Resource::Entry::AddFile(const std::string& name, const Path& src) {
    AddFile(name, src, false);
}

void Resource::Entry::AddFile(
        const std::string& name,
        const Path& src,
        bool rename,
        boost::filesystem::perms perm) {
    AddFile(name, src, rename);
    Path dst = m_temp_dir / name;
    boost::system::error_code ec;
    boost::filesystem::permissions(dst, perm, ec);
    if (ec) {
        LOG(WARNING) << "Fail to change permission (" << dst << "): " << ec;
        m_base->m_status = kInternalError;
    }
}

void Resource::Entry::AddFile(const std::string& name, const Path& src,
                              boost::filesystem::perms perm) {
    AddFile(name, src);
    Path dst = m_temp_dir / name;
    boost::system::error_code ec;
    boost::filesystem::permissions(dst, perm, ec);
    if (ec) {
        LOG(WARNING) << "Fail to change permission (" << dst << "): " << ec;
        m_base->m_status = kInternalError;
    }
}

void Resource::Entry::DumpFile(const std::string& name, const char* buffer, size_t length,
                               boost::filesystem::perms perm) {
    if (m_files.count(name) || m_sub_entries.count(name)) {
        m_base->m_status = kNameExists;
        LOG(WARNING) << "Trying to add duplicated file: " << (m_temp_dir / name);
        return;
    }
    Path path = m_temp_dir / name;

    boost::filesystem::ofstream stream(path, std::ios_base::trunc | std::ios_base::binary);
    stream.write(buffer, length).flush();
    if (!stream.good()) {
        m_base->m_status = kInternalError;
        LOG(WARNING) << "Fail to dump contents to file (" << path << ")";
        return;
    }
    m_files.insert(name);

    boost::system::error_code ec;
    boost::filesystem::permissions(path, perm, ec);
    if (ec) {
        LOG(WARNING) << "Fail to change permission (" << path << "): " << ec;
        m_base->m_status = kInternalError;
    }
}

Resource::Entry* Resource::Entry::GetEntry(const std::string& name) {
    boost::ptr_map<std::string, Entry>::iterator ptr = m_sub_entries.find(name);
    if (ptr == m_sub_entries.end()) {
        boost::system::error_code ec;

        Path path = m_temp_dir / name;  // concat path
        if (!boost::filesystem::create_directory(path, ec)) {
            LOG(WARNING) << "Fail to create directory (" << path << "): " << ec;
            m_base->m_status = kInternalError;
            return NULL;
        }

        Entry* entry = new Entry(name, path, m_base);
        m_sub_entries.insert(name, std::auto_ptr<Entry>(entry));
        return entry;
    }

    // Do not Check this now.

    // if (m_base->m_reserved_entries.count(ptr->second) != 0) {
    //     // Forbiden direct accesing to reserved entry
    //     LOG(FATAL) << "Trying to access reserved entry directly: ./" << name;
    //     return NULL;
    // }
    return ptr->second;
}

void Resource::Entry::AddNormalFile(const std::string& name, const std::string& path) {
    boost::filesystem::perms perm =
            boost::filesystem::owner_read | boost::filesystem::owner_write |
            boost::filesystem::group_read;
    bool rename = false;
    AddFile(name, path, rename, perm);
}

void Resource::Entry::AddNormalFileFromBytes(const std::string& name,
                                             const char* buffer, size_t length) {
    boost::filesystem::perms perm =
            boost::filesystem::owner_read | boost::filesystem::owner_write |
            boost::filesystem::group_read;
    DumpFile(name, buffer, length, perm);
}

void Resource::Entry::AddNormalFileByRename(const std::string& name, const std::string& path) {
    boost::filesystem::perms perm =
            boost::filesystem::owner_read | boost::filesystem::owner_write |
            boost::filesystem::group_read;
    bool rename = true;
    AddFile(name, path, rename, perm);
}

void Resource::Entry::AddExecutable(const std::string& name, const std::string& path) {
    boost::filesystem::perms perm =
            boost::filesystem::owner_read | boost::filesystem::owner_exe |
            boost::filesystem::group_read | boost::filesystem::group_exe;
    bool rename = false;
    AddFile(name, path, rename, perm);
}

void Resource::Entry::AddExecutableFromBytes(const std::string& name,
                                             const char* buffer, size_t length) {
    boost::filesystem::perms perm =
            boost::filesystem::owner_read | boost::filesystem::owner_exe |
            boost::filesystem::group_read | boost::filesystem::group_exe;
    DumpFile(name, buffer, length, perm);
}

std::string Resource::Entry::relative_path() const {
    std::string base = m_base->GetRootEntry()->m_temp_dir.string();
    std::string full = m_temp_dir.string();
    return full.substr(full.find_first_of(base) + base.size());
}

std::vector<std::string> Resource::Entry::ListFiles() {
    return std::vector<std::string>(m_files.begin(), m_files.end());
}

std::vector<Resource::Entry*> Resource::Entry::ListEntries() {
    std::vector<Resource::Entry*> results;
    boost::ptr_map<std::string, Entry>::iterator ptr = m_sub_entries.begin();
    while (ptr != m_sub_entries.end()) {
        results.push_back(ptr->second);
        ++ptr;
    }
    return results;
}

void Resource::AddDynamicLibrary(const std::string& name, const std::string& path) {
    m_lib_entry->AddExecutable(name, path);
}

void Resource::AddDynamicLibraryFromBytes(const std::string& name,
                                          const char* buffer, size_t length) {
    m_lib_entry->AddExecutableFromBytes(name, buffer, length);
}

std::vector<std::string> Resource::ListDynamicLibraries() {
    return m_lib_entry->ListFiles();
}

void Resource::AddJavaLibrary(const std::string& name, const std::string& path) {
    m_javalib_entry->AddNormalFile(name, path);
}

void Resource::AddJavaLibraryFromBytes(const std::string& name, const char* buffer, size_t length) {
    m_javalib_entry->AddNormalFileFromBytes(name, buffer, length);
}

std::vector<std::string> Resource::ListJavaLibraries() {
    return m_javalib_entry->ListFiles();
}

void Resource::AddPythonLibrary(const std::string& name, const std::string& path) {
    m_pythonlib_entry->AddNormalFile(name, path);
}

void Resource::AddPythonLibraryFromBytes(const std::string& name, const char* buffer, size_t length) {
    m_pythonlib_entry->AddNormalFileFromBytes(name, buffer, length);
}

std::vector<std::string> Resource::ListPythonLibraries() {
    return m_pythonlib_entry->ListFiles();
}

void Resource::AddFile(const std::string& name, const std::string& path) {
    Path dst_path(name);
    Path src_path(path);

    Entry* entry = WalkThrough(m_root_entry.get(), dst_path.parent_path());

    entry->AddFile(dst_path.filename().string(), src_path);
}

void Resource::AddFileFromBytes(const std::string& name, const char* buffer, size_t length) {
    Path dst_path(name);

    Entry* entry = WalkThrough(m_root_entry.get(), dst_path.parent_path());

    entry->AddNormalFileFromBytes(dst_path.filename().string(), buffer, length);
}

void Resource::AddFileWithExecutePermission(const std::string& name, const std::string& path) {
    Path dst_path(name);
    Path src_path(path);

    Entry* entry = WalkThrough(m_root_entry.get(), dst_path.parent_path());

    boost::filesystem::perms permission =
        boost::filesystem::status(src_path).permissions() |
        boost::filesystem::owner_exe | boost::filesystem::group_exe;

    entry->AddFile(dst_path.filename().string(), src_path, permission);
}

Resource::Entry* Resource::GetFlumeEntry() {
    return m_flume_entry;
}

std::string Resource::ViewAsDirectory() {
    return m_temp_dir.string();
}

Resource::Entry* Resource::WalkThrough(Resource::Entry* base, const Path& path) {
    std::vector<std::string> elems;
    Path::iterator path_itr = path.begin();

    // remove "." , ".." and "/"
    for (; path_itr != path.end(); ++path_itr) {
        if (*path_itr == "." || *path_itr == "/") {
            continue;
        } else if (*path_itr == "..") {
            CHECK_NE(0, elems.size());
            elems.pop_back();
        } else {
            elems.push_back(path_itr->string());
        }
    }

    Entry* current = base;
    std::vector<std::string>::iterator it = elems.begin();
    for (; it != elems.end(); ++it) {
        current = current->GetEntry(*it);
    }
    return current;
}

void Resource::SetCacheArchiveList(const std::string &archive_list) {
    m_cache_archives = archive_list;
}

void Resource::SetCacheFileList(const std::string &file_list) {
    m_cache_files = file_list;
}

std::string Resource::GetCacheArchiveList() {
    return m_cache_archives;
}

std::string Resource::GetCacheFileList() {
    return m_cache_files;
}

}  // namespace runtime
}  // namespace flume
}  // namespace baidu
