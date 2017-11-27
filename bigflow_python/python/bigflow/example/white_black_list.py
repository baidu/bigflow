# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!filecoding:utf-8
"""
文件A中有一些用户消费数据，文件B中有一些IP黑名单，文件C中有一些用户名白名单。
现需要把全部用户消费记录中找出在白名单中出现过，且在黑名单中没有出现过的记录，并从剩下的记录中统计出所有用户的总消费值。

输入
文件A、B、C内容内的字符串中不含空格。
文件A中的数据有三列：用户名，ip，消费金额。消费金额都是整数。三列用空格格开。(数据条数不超过100万）
文件B中的数据每行只有一个字符串，即黑名单ip。（数据条数不超过10万）
文件C中的数据每行只有一个字符串，即白名单用户名。（数据条数不超过10万）

文件A:
user1 59.69.122.122 1000
user1 10.69.122.123 2000
user2 4.4.4.5 1000
user3 4.4.4.5 1021

文件B:
59.69.122.122
1.1.1.1

文件C:
user1
user3
user4

输出
将消费总和输出到标准输出。
3021
"""

import os
from bigflow import base, input, output
p = base.Pipeline.create('LOCAL')

dir = os.path.dirname(os.path.abspath(__file__)) + "/data/"
(A, B, C) = (dir + "A.text", dir + "B.text", dir + "C.text")
records = p.read(input.TextFile(A)).map(lambda _: _.split()) # user, ip, cost
ip_blacklist = p.read(input.TextFile(B)).map(lambda _:(_, None))
user_whitelist = p.read(input.TextFile(C)).map(lambda _:(_, None))

print records.map(lambda _: (_[1], (_[0], int(_[2])))) \
        .cogroup(ip_blacklist) \
        .apply_values(lambda records, ips: records.filter(lambda _, cnt: cnt == 0, ips.count())) \
        .flatten() \
        .map(lambda _: _[1]) \
        .cogroup(user_whitelist) \
        .apply_values(lambda records, users: records.filter(lambda _, cnt: cnt != 0, users.count())) \
        .flatten_values() \
        .sum() \
        .get()
