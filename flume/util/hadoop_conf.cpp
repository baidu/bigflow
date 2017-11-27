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
// Author: Zhang Yuncong (zhangyuncong@baidu.com)
//
// Description: Flume hadoop conf util
#include <map>
#include <string>

#include "boost/scope_exit.hpp"
#include "glog/logging.h"
#include "tinyxml2.h"
#include "toft/storage/file/file.h"

#include "flume/util/hadoop_conf.h"

namespace baidu {
namespace flume {
namespace util {

namespace {

bool ParseProperty(tinyxml2::XMLElement* node, std::string* name, std::string* value) {
    const char* kName = "name";
    const char* kValue = "value";
    tinyxml2::XMLElement* name_element = node->FirstChildElement(kName);
    tinyxml2::XMLElement* value_element = node->FirstChildElement(kValue);
    if (NULL == name_element || NULL == value_element) {
        return false;
    }
    const char* name_text = name_element->GetText();
    const char* value_text = value_element->GetText();
    if (NULL == name_text) {
        return false;
    }
    *name = name_text;
    // value_text may be NULL when the value is an empty xml tag <value/>.
    if (value_text != NULL) {
        *value = value_text;
    } else {
        value->clear();
    }
    return true;
}

bool ParseProperties(tinyxml2::XMLDocument* doc, std::map<std::string, std::string>* properties) {
    tinyxml2::XMLElement* root = doc->RootElement();
    const char* kProperty = "property";
    std::string name;
    std::string value;
    for (tinyxml2::XMLElement* child = root->FirstChildElement(kProperty);
        child != NULL;
        child = child->NextSiblingElement(kProperty)) {
        if(!ParseProperty(child, &name, &value)) {
            return false;
        }
        (*properties)[name] = value;
    }
    return true;
}

} // namespace

HadoopConf::HadoopConf(const std::string& conf_file_path, const JobConfMap& jobconf) :
        m_hadoop_conf_path(conf_file_path),
        m_hadoop_job_ugi(""),
        m_fs_defaultfs("") {
    load_from_conffile(conf_file_path);
    load_from_sysenv();
    load_from_map(jobconf);
}

HadoopConf::HadoopConf(const std::string& conf_file_path) :
        m_hadoop_conf_path(conf_file_path),
        m_hadoop_job_ugi(""),
        m_fs_defaultfs("") {
    load_from_conffile(conf_file_path);
    load_from_sysenv();
}

HadoopConf::HadoopConf(const JobConfMap& jobconf) :
    m_hadoop_conf_path(""),
    m_hadoop_job_ugi(""),
    m_fs_defaultfs("") {
        load_from_map(jobconf);
}

void HadoopConf::load_from_map(const JobConfMap& jobconf) {
    JobConfMap::const_iterator it = jobconf.find(kFsDefaultName);
    if (it != jobconf.end()) {
        m_fs_defaultfs = it->second;
    }

    it = jobconf.find(kHadoopJobUgi);
    if (it != jobconf.end()) {
        m_hadoop_job_ugi = it->second;
    }
}

void HadoopConf::load_from_sysenv() {
    char* fs_defaultfs = getenv(kFsDefaultName);
    char* hadoop_job_ugi = getenv(kHadoopJobUgi);

    if (fs_defaultfs != NULL) {
        m_fs_defaultfs = fs_defaultfs;
    }
    if (hadoop_job_ugi != NULL) {
        m_hadoop_job_ugi = hadoop_job_ugi;
    }
}

void HadoopConf::load_from_conffile(const std::string& conf_file_path) {
    LOG(INFO) << "Get HadoopConf : " << m_hadoop_conf_path;
    tinyxml2::XMLDocument doc;
    CHECK_EQ(doc.LoadFile(conf_file_path.c_str()), tinyxml2::XMLError::XML_SUCCESS) << " at " << conf_file_path;
    std::map<std::string, std::string> properties;
    CHECK(ParseProperties(&doc, &properties));

    if (1u == properties.count(kFsDefaultName)) {
        m_fs_defaultfs = properties[kFsDefaultName];
    } else {
        LOG(WARNING) << "Found " << properties.count(kFsDefaultName)
            << " '" << kFsDefaultName << "' properties in HadoopConf file, ignore.";
    }
    if (1u == properties.count(kHadoopJobUgi)) {
        m_hadoop_job_ugi = properties[kHadoopJobUgi];
    } else {
        LOG(WARNING) << "Found " << properties.count(kHadoopJobUgi)
            << " '" << kHadoopJobUgi << "' properties in HadoopConf file, ignore.";
    }
}

} // namespace util
} // namespace flume
} // namespace baidu

