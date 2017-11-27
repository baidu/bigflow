#!/usr/bin/env python
# encoding: utf-8

########################################################################
#
# Copyright (c) 2016 Baidu, Inc. All Rights Reserved.

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

from bigflow import transforms


def column_sum(pcollection, columns):
    """
    对输入的PCollection，求所有元素按指定列相加的结果

    Args:
      pcollection (PCollection):  输入PCollection
      columns(list)：要计算的列

    Returns:
      PObject:  聚合结果

    >>> import columns
    >>> _p = _pipeline.parallelize([(1, 1, 1), (1, 2, 2), (1, 3, 1)])
    >>> columns.column_sum(_p, [0, 1]).get()
    [3, 6]
    """

    cols = columns

    def _get_columns(record):
        return [record[column] for column in cols]

    return pcollection.map(_get_columns) \
                      .reduce(lambda x, y: [a + b for a, b in zip(x, y)])
