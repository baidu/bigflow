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
Bigflow Python代码示例: 实现PageRank

Author: Wang, Cong(bigflow-opensource@baidu.com)
"""

import logging

from bigflow import base
from bigflow import transforms


def pagerank_algo(edges):
    """
    接受表示网页链接关系的PCollection，求出每个网页的Rank指标

    Args:
      edges (PCollection):  输入PCollection，格式为(from_page_id, to_page_id)

    Result:
      PCollection:  表示结果的PCollection，格式为(page_id, rank)
    """
    links = edges.apply(transforms.group_by_key)

    ranks = links.apply(transforms.extract_keys) \
                 .apply(transforms.map, lambda x: (x, 1))

    iteration_times = 5

    for i in xrange(0, iteration_times):
        joined = edges.join(ranks)

        ranks = joined.map(lambda x: x[1]) \
                      .group_by_key() \
                      .apply_values(transforms.sum) \
                      .flatten()

    return ranks

if __name__ == "__main__":
    logging.basicConfig(leve="DEBUG")

    _pipeline = base.Pipeline.create("LOCAL")

    input_data = [(0, 0), (0, 1), (0, 3), (0, 4),
                  (1, 0), (1, 1),
                  (2, 0), (2, 2),
                  (3, 3),
                  (4, 0), (4, 1), (4, 4)]

    edges = _pipeline.parallelize(input_data)

    result = edges.apply(pagerank_algo)

    print result.get()
