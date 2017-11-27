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

#ifndef FLUME_SERVICE_COMMON_PROFILER_UTIL_H_
#define FLUME_SERVICE_COMMON_PROFILER_UTIL_H_

#include <map>
#include <string>

#include "glog/logging.h"

#include "toft/compress/block/snappy.h"

namespace baidu {
namespace flume {
namespace service {

class ProfilingEntry {
public:
    enum Type {
        CPU = 0,
        MEMORY,
    };

    ProfilingEntry(Type type, std::string value)
        : m_type(type), m_value(value), m_compressed_value(""), m_is_compressed(false) {
    }

    ProfilingEntry(Type type, std::string value, bool is_compressed)
        : m_type(type), m_is_compressed(is_compressed) {
        if (is_compressed) {
            m_compressed_value = value;
            m_value.clear();
        } else {
            m_value = value;
            m_compressed_value.clear();
        }
    }

    ~ProfilingEntry() {}

    Type GetType() const {
        return m_type;
    }

    std::string GetValue() {
        if (m_value.empty()) {
            toft::SnappyCompression().Uncompress(
                    m_compressed_value.c_str(), m_compressed_value.size(), &m_value);
        }
        return m_value;
    }

    std::string GetCompressedValue() {
        if (m_compressed_value.empty()) {
            toft::SnappyCompression().Compress(
                    m_value.c_str(), m_value.size(), &m_compressed_value);
        }
        return m_compressed_value;
    }

private:
    Type m_type;
    std::string m_value;
    std::string m_compressed_value;
    bool m_is_compressed;
};

typedef std::map<ProfilingEntry::Type, ProfilingEntry> ProfilingEntryMap;

class ProfilerUtil {
public:
    static void StartProfiling(const std::string& profile_file);

    static void StopProfiling();

    static bool ShouldDoProfiling(unsigned id);

    // Get the base path for profiling storage. It may:
    //   1. return an empty string if parameter not set
    //   2. return an un-empty path string ended with '/' character
    static std::string GetStorageBasePath();

    static bool StoreProfilings(const std::string& fpath,
                                const ProfilingEntryMap& entries);

    static void StartHeapProfiling(const std::string& profile_file);

    static void DumpHeapProfile(const char* reason);

    static void StopHeapProfiling();

    // Create profile directory to save profile file.
    // Create dot directory to save dot file.
    static void InitProfilingEnvironment(const std::string& profile_directory,
                                         const std::string& dot_directory);

    static void StartProfilingTask(const bool do_cpu_profile,
                                   const bool do_heap_profile,
                                   const std::string& task_log_directory,
                                   const std::string& cpu_prof_basename,
                                   const std::string& heap_prof_basename);

    static void StopProfilingTask(const bool do_cpu_profile,
                                  const bool do_heap_profile,
                                  const std::string& task_log_directory);

private:
    static std::string GetProfileDirectory(const std::string& task_log_directory,
                                           const std::string& profile_type);

    static std::string GetDotDirectory(const std::string& task_log_directory,
                                       const std::string& dot_type);

    // list file of profile directory, and call `pprof` script to
    // generate dot file in  dot directory.
    static void GenerateDotFile(const std::string& profile_directory,
                                const std::string& dot_directory);
};

}  // namespace service
}  // namespace flume
}  // namespace baidu

#endif  // FLUME_SERVICE_COMMON_PROFILER_UTIL_H_

