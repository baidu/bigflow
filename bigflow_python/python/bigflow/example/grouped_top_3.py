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
Bigflow Python代码示例: 对数据进行分组，求每组前3大的元素

Author: Pan, Yunhong(bigflow-opensource@baidu.com)
"""

import logging

from bigflow import base
from bigflow import transforms


def grouped_top_3(numbers):
    """
    接受包含数字的PCollection，按照模4取余的结果对数字分组，计算每组最大的3个数字并将
    结果通过(group, number)形式返回

    Args:
      numbers (PCollection):  输入PCollection，必须全部为数字

    Returns:
      PCollection:  输出

    >>> input_data = _pipeline.parallelize([1, 2, 3, 9, 8, 7, 4, 5, 6, 10, 11, 12, 15, 14, 13])
    >>> print input_data.apply(grouped_top_3).get()
    [(0, 4), (0, 8), (0, 12), (3, 7), (3, 11), (3, 15), (2, 6), (2, 10), (2, 14), (1, 5), (1, 9),
    (1, 13)]

    """
    group_by_mod4 = numbers.group_by(lambda c: c % 4, lambda c: c) \
        .apply_values(transforms.max_elements, 3) \
        .flatten()

    return group_by_mod4

if __name__ == "__main__":
    _pipeline = base.Pipeline.create("LOCAL")

    input_data = [1, 2, 3, 9, 8, 7, 4, 5, 6, 10, 11, 12, 15, 14, 13]
    numbers = _pipeline.parallelize(input_data)
    result = numbers.apply(grouped_top_3)
    print result.get()
