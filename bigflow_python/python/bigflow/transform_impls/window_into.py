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
"""
Script Definition

"""

from bigflow import pcollection
from bigflow import ptable
from bigflow import window


def node_window_by(
        node,
        win,
        concurrency=None,
        pipeline=None):
    """
    group by window, for internal use
    """
    plan = node.plan()
    scope = node.scope()

    shuffle = plan.shuffle(scope, [node])

    if concurrency is not None:
        shuffle.with_concurrency(concurrency)
    elif pipeline is not None and pipeline.estimate_concurrency:
        concurrency = node.size() / pipeline.size_per_concurrency
        shuffle.with_concurrency(concurrency)

    process_node = None
    if isinstance(win, window.DefaultWindow):
        shuffle_node = shuffle.node(0).distribute_as_batch()
        process_node = shuffle_node.set_debug_info("DefaultWindow")
    else:
        #FIXIT(zhangyuncong): only for test for now. Will fix before release.
        from bigflow.windowing import after_processing_time
        from bigflow.core import entity

        if win.trigger() is None:
            # just for test for now, at the final version it should be after watermark.
            shuffle_node = shuffle.window_by(win, after_processing_time.AfterProcessingTime(10)).node(0)
        else:
            shuffle_node = shuffle.window_by(win, win.trigger()).node(0)

        shuffle_node = shuffle_node.time_by(entity.PythonTimeReaderDelegator(lambda x: x[0]))
        process_node = shuffle_node.set_debug_info("Window")

    return process_node


def window_into(pvalue, win, **options):
    """
    group by window
    """
    pipeline = pvalue.pipeline()
    key_serde = options.get('key_serde', win.key_serde())
    if not key_serde:
        key_serde = pvalue.pipeline().default_objector()

    node = node_window_by(
            pvalue.node(),
            win,
            options.get('concurrency', None),
            pipeline)

    return ptable.PTable(pcollection.PCollection(node, pipeline), key_serde=key_serde)
