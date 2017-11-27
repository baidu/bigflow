#!/usr/bin/env python
# -*- coding: utf-8 -*-
########################################################################
#
# Copyright (c) 2015 Baidu, Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
########################################################################
"""
Bigflow Python实现word count示例

示例展示了如何使用Bigflow Python API实现分布式计算中经典的word count，主要包括4步:

 1. 使用 ``Pipeline.create()`` 方法创建一个Pipeline实例 ``_p`` 。
 2. 通过 ``_p.read()`` 方法读取HDFS文本文件，得到输入PCollection
 3. 对PCollection应用 :func:`.count_words_in_pcollection` 算法，算法由Bigflow提供的基本变换拼接而成
 4. 使用 ``_p.write()`` 方法将结果写出

"""
import logging
import os


from bigflow import base
from bigflow import transforms


def count_words(lines):
    """
    将输入PCollection的每个元素(文本文件的每一行)切分成单词，统计每个单词个数，返回
    内容为(word, count)的PCollection

    Args:
      lines (PCollection):  通过读取文本文件构造的PCollection

    Returns:
      PCollection:  表示统计结果的PCollection

    >>> _p = _pipeline.parallelize("to be or not to be")
    >>> print _p.apply(count_words).get()
    [("to", 2), ("be", 2), ("not", 1), ("or", 1)]
    """
    return lines.flat_map(lambda line: line.split(), memory=1000) \
        .map(lambda word: (word, 1), memory=1024) \
        .group_by_key() \
        .apply_values(transforms.count, memory=500) \
        .flatten()


if __name__ == "__main__":
    input_uri_prefix = '%s/%s' % ("", os.getcwd())
    output_uri_prefix = '%s/%s' % ("", os.getcwd())

    _pipeline = base.Pipeline.create("LOCAL")

    #lines = _pipeline.read(input.TextFile(input_uri_prefix + "/input/hamlet.txt",
    #                                      input_uri_prefix + "/input/hamlet-change.txt"))

    lines = _pipeline.parallelize(["A", "B"])

    result = lines.apply(count_words)

    # _pipeline.write(result, io.output.TextFile("output"))

    print result.get()
    # _pipeline.write(result, output.TextFile(output_uri_prefix + "/output"))
    # _pipeline.run()
