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
// Author: Wang Song <wangsong06@baidu.com>
//

#ifndef FLUME_UTIL_PROPERTY_DECLARER_H_
#define FLUME_UTIL_PROPERTY_DECLARER_H_

#ifndef FLUME_DECLARE_PROPERTY
#define FLUME_DECLARE_PROPERTY(type, name) \
public:\
    void set_##name(const type& val) { m_##name = val; }\
    const type& name() const { return m_##name; }\
    type* mutable_##name() { return &m_##name; }\
private:\
    type m_##name;
#endif // #ifndef FLUME_DECLARE_PROPERTY

#endif // FLUME_UTIL_PROPERTY_DECLARER_H_
