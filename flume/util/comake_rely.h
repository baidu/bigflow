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
// Description: Flume Hadoop Conf

// Read Conf from $HADOOP_CONF_PATH. if this environment variable is not set, use $HADOOP_HOME/etc/hadoop/core-site.xml instead.
// This action is same as the hadoop-client if we don't set the flag "--conf".

#ifndef FLUME_UTIL_HADOOP_CONF_H
#define FLUME_UTIL_HADOOP_CONF_H

#define COMAKE_RELY(class_name) delete new class_name

#endif  // FLUME_UTIL_HADOOP_CONF_H

