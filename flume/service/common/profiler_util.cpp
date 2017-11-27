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
// Author: Guo Yezhi <bigflow-opensource@baidu.com>

#include "flume/service/common/profiler_util.h"

#include <sys/stat.h>

#include <algorithm>

#include "boost/filesystem.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "perftools/profiler.h"
#include "perftools/heap-profiler.h"
#include "toft/base/scoped_ptr.h"
#include "toft/storage/file/local_file.h"
#include "toft/storage/path/path.h"
#include "toft/system/process/this_process.h"

DEFINE_int32(flume_profiling_sample_interval, 0,
             "profiling sample interval, profiling is always disabled if set less than 1");
DEFINE_string(flume_profiling_storage_base, "./profiling_data", "base path for profiling storage");

namespace baidu {
namespace flume {
namespace service {

void ProfilerUtil::InitProfilingEnvironment(const std::string& profile_directory,
                                            const std::string& dot_directory) {
    LOG(INFO) << "Current work path: " << toft::Path::GetCwd();

    if (boost::filesystem::exists(profile_directory)) {
        if (!boost::filesystem::is_directory(profile_directory)) {
            LOG(ERROR) << "The path of `profile_directory` has existsed, "\
                "and isn't a directory.";
            return;
        }
    } else {
        if (!boost::filesystem::create_directory(profile_directory)) {
            LOG(ERROR) << "Can't create profile directory !";
            return;
        }
    }

    if (boost::filesystem::exists(dot_directory)) {
        if (!boost::filesystem::is_directory(dot_directory)) {
            LOG(ERROR) << "The path of `dot_directory` has existsed, "\
                "and isn't a directory.";
            return;
        }
    } else {
        if (!boost::filesystem::create_directory(dot_directory)) {
            LOG(ERROR) << "Can't create dot directory !";
            return;
        }
    }

    LOG(INFO) << "Create profile directory and dot directory succeed!";

}

void ProfilerUtil::GenerateDotFile(const std::string& profile_directory,
                                   const std::string& dot_directory) {
    // pprof is a perl script.
    std::string pprof_script = "./flume/pprof";
    std::string tools = "-tools=nm:/opt/compiler/gcc-4.8.2/bin/nm,"\
                        "c++filt:/opt/compiler/gcc-4.8.2/bin/c++filt,"\
                        "addr2line:/opt/compiler/gcc-4.8.2/bin/addr2line,"\
                        "objdump:/opt/compiler/gcc-4.8.2/bin/objdump";

    std::vector<std::string> base_args;
    base_args.push_back(pprof_script);
    base_args.push_back(tools);
    base_args.push_back("--dot");
    base_args.push_back(toft::ThisProcess::BinaryPath());

    LOG(INFO) << "foreach profile_directory: " << profile_directory;

    // Singleton, don't free the pointer `fs`.
    toft::FileSystem* fs = toft::File::GetFileSystemByPath(profile_directory);
    toft::scoped_ptr<toft::FileIterator> file_iter(
            fs->Iterate(profile_directory, "*", toft::FileType_All, toft::FileType_Directory));
    toft::FileEntry entry;

    CHECK(file_iter) << "Create Profile Directory Iterator Error !";

    while (file_iter->GetNext(&entry)) {
        LOG(INFO) << "Profile name: " << entry.name;

        std::string cmd;

        std::vector<std::string> args = base_args;

        std::string profile = toft::Path::Join(profile_directory, entry.name);
        std::string dotfile = toft::Path::Join(dot_directory, entry.name + ".dot");

        if (boost::filesystem::exists(profile)) {
            if (boost::filesystem::is_directory(profile)) {
                LOG(ERROR) << "Profile is a directory, continue.";
                continue;
            } else {
               LOG(INFO) << "Profile is a file, ready to generate dot file.";
            }
        } else {
            LOG(ERROR) << "Profile was not existsed, continue.";
            continue;
        }

        args.push_back(profile);
        args.push_back(">");
        args.push_back(dotfile);

        for (size_t i = 0; i < args.size(); i++) {
            cmd += args[i] + " ";
        }

        LOG(INFO) << "Generate dot file shell command: " << cmd;

        LOG(INFO) << "Call shell command return: " << system(cmd.c_str());
    }

    LOG(INFO) << "Done Generate Dot File Job!";

}

void ProfilerUtil::StartHeapProfiling(const std::string& profile_file) {
    HeapProfilerStart(profile_file.c_str());
}

void ProfilerUtil::DumpHeapProfile(const char* reason) {
    HeapProfilerDump(reason);
}

void ProfilerUtil::StopHeapProfiling() {
    HeapProfilerStop();
}

void ProfilerUtil::StartProfiling(const std::string& profile_file) {
    ProfilerStart(profile_file.c_str());
}

void ProfilerUtil::StopProfiling() {
    ProfilerStop();
}

bool ProfilerUtil::ShouldDoProfiling(unsigned id) {
    // profiling is always disabled if set less than 1
    if (FLAGS_flume_profiling_sample_interval < 1) {
        return false;
    }

    // profile 1 in every 'interval' tasks
    return (id % FLAGS_flume_profiling_sample_interval == 0);
}

std::string ProfilerUtil::GetStorageBasePath() {
    if (FLAGS_flume_profiling_storage_base.empty()) {
        return "";
    }
    std::string res = FLAGS_flume_profiling_storage_base;
    if (FLAGS_flume_profiling_storage_base.find_last_of("/") !=
            (FLAGS_flume_profiling_storage_base.length() - 1)) {
        res.append("/");
    }
    if (!toft::File::Exists(res) && !toft::LocalFileSystem().Mkdir(res, S_IRWXU | S_IRWXG)) {
        LOG(ERROR) << res << " mkdir failed!";
        return "";
    }

    return res;
}

bool ProfilerUtil::StoreProfilings(const std::string& fpath,
                                   const ProfilingEntryMap& entries) {
    typedef ProfilingEntryMap::const_iterator Iterator;
    for (Iterator itor = entries.begin(); itor != entries.end(); ++itor) {
        bool res = true;
        std::string suffix;
        switch (itor->first) {
        case ProfilingEntry::CPU:
            suffix = ".cpu";
            break;
        case ProfilingEntry::MEMORY:
            suffix = ".memory";
            break;
        default:
            LOG(ERROR) << "Not Support type!";
            return false;
        }

        toft::File* file = toft::File::Open(fpath + suffix, "w");
        const std::string& value = const_cast<ProfilingEntry*>(&itor->second)->GetValue();
        if (file->Write(value.c_str(), value.size()) == -1 || !file->Close()) {
            LOG(WARNING) << "Could not complete writing";
            res = false;
        }
        delete file;

        if (!res) {
            return false;
        }
    }

    return true;
}

std::string ProfilerUtil::GetProfileDirectory(const std::string& task_log_directory,
                                              const std::string& profile_type) {
    return toft::Path::Join(task_log_directory, profile_type);
}

std::string ProfilerUtil::GetDotDirectory(const std::string& task_log_directory,
                                          const std::string& dot_type) {
    return toft::Path::Join(task_log_directory, dot_type);
}

void ProfilerUtil::StartProfilingTask(const bool do_cpu_profile,
                                      const bool do_heap_profile,
                                      const std::string& task_log_directory,
                                      const std::string& cpu_prof_basename,
                                      const std::string& heap_prof_basename) {
    if (do_cpu_profile) {
        const std::string cpu_profile_directory;
        const std::string cpu_dot_directory;
        cpu_profile_directory = GetProfileDirectory(task_log_directory, "cpu_profile");
        cpu_dot_directory = GetDotDirectory(task_log_directory, "cpu_dot");

        service::ProfilerUtil::InitProfilingEnvironment(
                cpu_profile_directory, cpu_dot_directory);

        const std::string cpu_profile_path = \
            toft::Path::Join(cpu_profile_directory, cpu_prof_basename);

        service::ProfilerUtil::StartProfiling(cpu_profile_path);
    }

    if (do_heap_profile) {
        const std::string heap_profile_directory;
        const std::string heap_dot_directory;
        heap_profile_directory = GetProfileDirectory(task_log_directory, "heap_profile");
        heap_dot_directory = GetDotDirectory(task_log_directory, "heap_dot");

        service::ProfilerUtil::InitProfilingEnvironment(
                heap_profile_directory, heap_dot_directory);


        const std::string heap_profile_path = \
            toft::Path::Join(heap_profile_directory, heap_prof_basename);

        service::ProfilerUtil::StartHeapProfiling(heap_profile_path);
    }
}

void ProfilerUtil::StopProfilingTask(const bool do_cpu_profile,
                                     const bool do_heap_profile,
                                     const std::string& task_log_directory) {
    if (do_cpu_profile) {
        service::ProfilerUtil::StopProfiling();
        service::ProfilerUtil::GenerateDotFile(GetProfileDirectory(task_log_directory, "cpu_profile"),
                                               GetDotDirectory(task_log_directory, "cpu_dot"));
    }

    if (do_heap_profile) {
        service::ProfilerUtil::DumpHeapProfile("finish task.");
        service::ProfilerUtil::StopHeapProfiling();
        service::ProfilerUtil::GenerateDotFile(GetProfileDirectory(task_log_directory, "heap_profile"),
                                               GetDotDirectory(task_log_directory, "heap_dot"));
    }
}

}  // namespace service
}  // namespace flume
}  // namespace baidu

