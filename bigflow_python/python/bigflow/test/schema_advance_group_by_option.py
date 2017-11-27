#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
"""
"""

import unittest

from bigflow.test import test_base
from bigflow import input
from bigflow import transforms
from bigflow import schema

class TestCase(test_base.MRTestCaseWithMockClient):
    """
    test schema group_by option.
    group_by option only support value_serde and concurrency
    """

    def test_concurrency(self):
        """ test """
        client = self.get_mock_client()

        pipeline = self.create_pipeline(tmp_data_path='/app/dc/yarn/bigflow/acmol/',
                                        default_concurrency = 100)

        raw_data = [("a", 1), ("b", 4), ("a", 2), ("a", 3), ("b", 5), ("b", 6)]

        pipeline.parallelize(raw_data) \
                .as_schema(["key", "value"]) \
                .apply(schema.group_by, ["key"], concurrency = 200) \
                .apply_values(schema.agg,
                              lambda cols: {"key": cols["key"], "sum": cols["value"].sum()}) \
                .apply_values(transforms.first) \
                .apply(schema.flatten) \
                .cache()

        pipeline.run()

        job = client.recent_job_cmds()[0]
        self.assertEqual(2, job.vertex_num())
        self.assertEqual(None, job.vertex_concurrency(0))
        self.assertEqual(200, job.vertex_concurrency(1))

if __name__ == "__main__":
    unittest.main()
