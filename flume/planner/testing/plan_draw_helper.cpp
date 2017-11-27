/***************************************************************************
 *
 * Copyright (c) 2014 Baidu, Inc. All Rights Reserved.
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
// Author: Pan Yuchang (BDG), panyuchang@baidu.com
// Last Modified: 08/13/14 21:15:54
// Description: See local_draw_plan.h description

#include "flume/planner/testing/plan_draw_helper.h"

#include <algorithm>
#include <iomanip>
#include <sstream>

#include "boost/filesystem.hpp"
#include "boost/filesystem/fstream.hpp"
#include "glog/logging.h"
#include "toft/base/closure.h"
#include "toft/base/string/algorithm.h"

#include "flume/planner/plan.h"

namespace baidu {
namespace flume {
namespace planner {

using boost::system::error_code;
using boost::filesystem::exists;
using boost::filesystem::create_directory;
using boost::filesystem::remove_all;
using boost::filesystem::remove;
using boost::filesystem::path;

PlanDraw::PlanDraw(const std::string& dot_file_dir)
        : m_round_count(0), m_is_valid(false), m_output_dir(dot_file_dir) {
    m_drawer.RegisterListener(toft::NewPermanentClosure(this, &PlanDraw::WriteToFile));
    m_is_valid = InitFileDir();
}

PlanDraw::~PlanDraw() {}

bool PlanDraw::Draw(Plan* plan) {
    if (m_is_valid) {
        m_drawer.Run(plan);
    }
    return false;
}

bool PlanDraw::InitFileDir() {
    error_code ec;
    path dir_path(m_output_dir);
    if (!exists(dir_path)) {
        if (!create_directories(dir_path, ec)) {
            LOG(ERROR) << "Can't create " << dir_path;
            LOG(ERROR) << ec;
            return false;
        }
    } else {
        path paths = dir_path / "*";
        remove_all(paths, ec);
        if (ec) {
            LOG(ERROR) << "Can't init " << dir_path;
            LOG(ERROR) << ec;
            return false;
        }
    }
    return true;
}

void PlanDraw::WriteToFile(const std::string& description) {
    std::ostringstream stream;
    stream << std::setfill('0') << std::setw(3) << m_round_count++ << ".dot";
    path dir_path(m_output_dir);
    path file_path = dir_path / stream.str();
    error_code ec;

    if (exists(file_path)) {
        if (!remove(file_path, ec)) {
            LOG(ERROR) << "Can't remove " << file_path;
            LOG(ERROR) << ec;
            return;
        }
    }

    boost::filesystem::ofstream out_stream(file_path);
    out_stream.write(description.c_str(), description.size()).flush();
    if (!out_stream.good()) {
        LOG(ERROR) << "ofstream out_stream error" << file_path;
    }
}

} // namespace planner
} // namespace flume
} // namespace baidu
