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
"""
Unit tests of LogicalPlan

"""

import unittest
import cPickle

import test_helper
from bigflow.core import logical_plan
from bigflow.core import entity
from flume.proto import logical_plan_pb2
from flume.proto import entity_pb2
from bigflow_python.proto import processor_pb2

from bigflow.core import entity_names

class Test(unittest.TestCase):

    def setUp(self):
        self.plan = logical_plan.LogicalPlan()
        self.assertEqual("", self.plan.global_scope().id(),
                         "Identity of global scope should be empty")

    def _finish_plan(self):
        self.message = self.plan.to_proto_message()
        self.vertices = dict()
        self.logical_dag = dict()
        self.scope_tree = dict()
        self.dag_tree = dict()

        test_helper.build_dag_tree(self.message, self.dag_tree, self.vertices)

        # print self.message

    def _proto_of(self, node_or_scope):
        """ Return Protobuf message of a LogicalPlan Node or Scope

        :param node_or_scope: Node or Scope
        :return: Protobuf message
        """
        return self.dag_tree[self.vertices[node_or_scope.id()]]

    def _assert_belongs_to(self, node_or_scope, scope):
        self.assertEqual(self.vertices[node_or_scope.id()].father, self.vertices[scope.id()])

    def _assert_from(self, to_node, from_node):
        self.assertTrue(from_node.id() in self.vertices[to_node.id()].froms)

    def tearDown(self):
        pass

    def test_load(self):
        __operator = entity.Entity

        plan = self.plan
        load = plan.load(["file1", "file2"])\
            .by(test_helper.TestLoader())\
            .as_type(test_helper.TestObjector())
        load.set_debug_info("test")

        self._finish_plan()

        self.assertEqual(1, len(self.message.node))
        self.assertEqual(2, len(self.message.scope))

        # Check load node
        load_message = self._proto_of(load)
        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.LOAD_NODE, load_message.type)
        #self.assertEqual("test", load_message.debug_info)

        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector()),
                         entity.Entity.from_message(load_message.objector))

        self.assertItemsEqual(["file1", "file2"], load_message.load_node.uri)
        self.assertEqual(entity.Entity.of(__operator.loader, test_helper.TestLoader()),
                         entity.Entity.from_message(load_message.load_node.loader))

        # Check load scope
        scope_message = self._proto_of(load.scope())
        self.assertFalse(scope_message.is_sorted)
        self.assertEqual(logical_plan_pb2.PbScope.INPUT, scope_message.type)
        self.assertEqual(entity.Entity.of(__operator.loader, test_helper.TestLoader()),
                         entity.Entity.from_message(scope_message.input_scope.spliter))
        self.assertItemsEqual(["file1", "file2"], scope_message.input_scope.uri)
        self._assert_belongs_to(load.scope(), self.plan.global_scope())
        self._assert_belongs_to(load, load.scope())

    def test_sink(self):
        __operator = entity.Entity

        plan = self.plan
        load = plan.load(["file"]).by(test_helper.TestLoader()).as_type(test_helper.TestObjector())
        sink = plan.sink(plan.global_scope(), load).by(test_helper.TestSinker())

        self._finish_plan()

        self.assertEqual(2, len(self.message.node))
        self.assertEqual(2, len(self.message.node))

        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.SINK_NODE, self._proto_of(sink).type)
        self.assertEqual(entity.Entity.of(__operator.sinker, test_helper.TestSinker()),
                         entity.Entity.from_message(self._proto_of(sink).sink_node.sinker))
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector()),
                         entity.Entity.from_message(self._proto_of(load).objector))

        self.assertFalse(self._proto_of(sink.scope()).is_sorted)

        self._assert_from(sink, load)
        self._assert_belongs_to(sink, self.plan.global_scope())

    def test_process(self):
        __operator = entity.Entity

        plan = self.plan
        source_1 = plan.load(["file1"]).by(test_helper.TestLoader())\
            .as_type(test_helper.TestObjector())
        source_2 = plan.load(["file2"]).by(test_helper.TestLoader())\
            .as_type(test_helper.TestObjector())

        process_1 = \
            source_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("config1"))\
            .input(0).allow_partial_processing()\
            .done()

        process_2 = \
            source_1.combine_with(process_1).by(test_helper.TestProcessor("conf2"))\
            .as_type(test_helper.TestObjector())\
            .input(0).allow_partial_processing()\
            .input(1).prepare_before_processing()\
            .done()

        process_3 = \
            source_1.combine_with(source_2).by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector())\
            .prepare_at_least(1)

        self._finish_plan()

        self.assertEqual(5, len(self.message.node))
        self.assertEqual(3, len(self.message.scope))

        # check process_1
        process_message = self._proto_of(process_1)
        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.PROCESS_NODE,
                         process_message.type)
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("config1")),
                         entity.Entity.from_message(process_message.objector))
        self.assertEqual(entity.Entity.of(__operator.processor, test_helper.TestProcessor()),
                         entity.Entity.from_message(process_message.process_node.processor))
        self.assertEqual(1, len(process_message.process_node.input))
        self.assertEqual(0, process_message.process_node.least_prepared_inputs)
        self.assertTrue(process_message.process_node.input[0].is_partial)
        self.assertFalse(process_message.process_node.input[0].is_prepared)

        self._assert_belongs_to(process_1, source_1.scope())
        self._assert_from(process_1, source_1)

        # check process_2
        process_message = self._proto_of(process_2)
        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.PROCESS_NODE,
                         process_message.type)
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector()),
                         entity.Entity.from_message(process_message.objector))
        self.assertEqual(entity.Entity.of(__operator.processor, test_helper.TestProcessor("conf2")),
                         entity.Entity.from_message(process_message.process_node.processor))
        self.assertEqual(2, len(process_message.process_node.input))
        self.assertEqual(0, process_message.process_node.least_prepared_inputs)
        self.assertTrue(process_message.process_node.input[0].is_partial)
        self.assertFalse(process_message.process_node.input[0].is_prepared)
        self.assertFalse(process_message.process_node.input[1].is_partial)
        self.assertTrue(process_message.process_node.input[1].is_prepared)

        self._assert_belongs_to(process_2, source_1.scope())
        self._assert_from(process_2, process_1)
        self._assert_from(process_2, source_1)

        # check process_3
        process_message = self._proto_of(process_3)
        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.PROCESS_NODE,
                         process_message.type)
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector()),
                         entity.Entity.from_message(process_message.objector))
        self.assertEqual(entity.Entity.of(__operator.processor, test_helper.TestProcessor()),
                         entity.Entity.from_message(process_message.process_node.processor))
        self.assertEqual(2, len(process_message.process_node.input))
        self.assertEqual(1, process_message.process_node.least_prepared_inputs)
        self.assertFalse(process_message.process_node.input[0].is_partial)
        self.assertFalse(process_message.process_node.input[0].is_prepared)
        self.assertFalse(process_message.process_node.input[1].is_partial)
        self.assertFalse(process_message.process_node.input[1].is_prepared)

        self._assert_belongs_to(process_3, self.plan.global_scope())
        self._assert_from(process_3, source_1)
        self._assert_from(process_3, source_2)

    def test_shuffle_by_key(self):
        __operator = entity.Entity

        plan = self.plan
        start_0_1 = plan.load(["file1"]).by(test_helper.TestLoader())\
            .as_type(test_helper.TestObjector())

        node_0_1 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("1"))
        node_0_2 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("2"))
        node_0_3 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("3"))

        shuffle_1 = plan.shuffle(start_0_1.scope(), [node_0_1, node_0_2, node_0_3])
        start_1_1 = shuffle_1.node(0).match_by(test_helper.TestKeyReader("1"))
        start_1_2 = shuffle_1.node(1).match_by(test_helper.TestKeyReader("2"))
        start_1_3 = shuffle_1.node(2).match_by(test_helper.TestKeyReader("3"))

        node_1_1 = start_1_1.sort_by(test_helper.TestKeyReader())  # implicit shuffle
        node_1_2 = start_1_2
        start_2_1 = start_1_3.group_by(test_helper.TestKeyReader())  # shuffle 2
        start_3_1 = start_1_3.sort_and_group_by(test_helper.TestKeyReader())  # shuffle 3

        shuffle_4 = plan.shuffle(shuffle_1.scope(), [node_1_1, node_1_2])\
            .node(0).match_by(test_helper.TestKeyReader())\
            .node(1).match_any()\
            .done()

        shuffle_5 = plan.shuffle(plan.global_scope(), [node_1_1, node_1_2])\
            .node(0).match_by(test_helper.TestKeyReader())\
            .node(1).match_any()\
            .done()

        self._finish_plan()

        self.assertEqual(15, len(self.message.node))
        self.assertEqual(8, len(self.message.scope))

        # check shuffle_1
        self.assertEqual(logical_plan_pb2.PbScope.GROUP,
                         self._proto_of(shuffle_1.scope()).type)
        self.assertFalse(self._proto_of(shuffle_1.scope()).is_sorted)
        self.assertFalse(self._proto_of(shuffle_1.scope()).HasField("concurrency"))
        self._assert_belongs_to(shuffle_1.scope(), start_0_1.scope())

        checklist = [node_0_1, start_1_1, node_0_2, start_1_2, node_0_3, start_1_3]
        for i in range(0, 3):
            from_node = checklist[i * 2]
            node = checklist[i * 2 + 1]

            self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE,
                             self._proto_of(node).type)
            self.assertEqual(logical_plan_pb2.PbShuffleNode.KEY,
                             self._proto_of(node).shuffle_node.type)
            self.assertEqual(
                entity.Entity.of(__operator.objector, test_helper.TestObjector(str(i + 1))),
                entity.Entity.from_message(self._proto_of(node).objector)
            )
            self.assertEqual(
                entity.Entity.of(__operator.key_reader, test_helper.TestKeyReader(str(i + 1))),
                entity.Entity.from_message(self._proto_of(node).shuffle_node.key_reader)
            )

            self._assert_belongs_to(node, shuffle_1.scope())
            self._assert_from(node, from_node)

        # check implicit shuffle
        node = node_1_1.from_node(0)

        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.UNION_NODE,
                         self._proto_of(node_1_1).type)
        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE,
                         self._proto_of(node).type)
        self.assertEqual(logical_plan_pb2.PbShuffleNode.KEY,
                         self._proto_of(node).shuffle_node.type)
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("1")),
                         entity.Entity.from_message(self._proto_of(node).objector))
        self.assertEqual(entity.Entity.of(__operator.sort_key_reader, test_helper.TestKeyReader()),
                         entity.Entity.from_message(self._proto_of(node).shuffle_node.key_reader))

        self.assertEqual(logical_plan_pb2.PbScope.GROUP,
                         self._proto_of(node.scope()).type)
        self.assertTrue(self._proto_of(node.scope()).is_sorted)
        self.assertFalse(self._proto_of(node.scope()).HasField("concurrency"))

        self._assert_from(node_1_1, node)
        self._assert_from(node, start_1_1)
        self._assert_belongs_to(node.scope(), shuffle_1.scope())
        self._assert_belongs_to(node, node.scope())

        # check shuffle 2/3
        checklist = [start_2_1, start_3_1]
        for i in range(0, 2):
            node = checklist[i]
            node_message = self._proto_of(node)
            node_scope_message = self._proto_of(node.scope())

            self.assertEqual(logical_plan_pb2.PbScope.GROUP, node_scope_message.type)
            self.assertEqual(bool(i), node_scope_message.is_sorted)
            self.assertFalse(node_scope_message.HasField("concurrency"))
            self._assert_belongs_to(node.scope(), shuffle_1.scope())

            self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE,
                             node_message.type)
            self.assertEqual(logical_plan_pb2.PbShuffleNode.KEY,
                             node_message.shuffle_node.type)
            self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("3")),
                             entity.Entity.from_message(node_message.objector))
            self.assertEqual(entity.Entity.of(__operator.key_reader, test_helper.TestKeyReader()),
                             entity.Entity.from_message(node_message.shuffle_node.key_reader))
            self._assert_belongs_to(node, node.scope())

        # check shuffle 4/5
        checklist = [shuffle_4, shuffle_5]
        scopes = [shuffle_4.scope(), shuffle_1.scope(), shuffle_5.scope(), plan.global_scope()]
        for i in range(0, 2):
            node_1 = checklist[i].node(0)
            node_2 = checklist[i].node(1)

            scope = scopes[2 * i]
            father_scope = scopes[2 * i + 1]

            self.assertEqual(logical_plan_pb2.PbScope.GROUP, self._proto_of(scope).type)
            self.assertFalse(self._proto_of(scope).is_sorted)
            self.assertFalse(self._proto_of(scope).HasField("concurrency"))
            self._assert_belongs_to(scope, father_scope)

            node_1_message = self._proto_of(node_1)
            self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE,
                             node_1_message.type)
            self.assertEqual(logical_plan_pb2.PbShuffleNode.KEY,
                             node_1_message.shuffle_node.type)
            self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("1")),
                             entity.Entity.from_message(node_1_message.objector))
            self.assertEqual(entity.Entity.of(__operator.key_reader, test_helper.TestKeyReader()),
                             entity.Entity.from_message(node_1_message.shuffle_node.key_reader))
            self._assert_belongs_to(node_1, scope)

            node_2_message = self._proto_of(node_2)
            self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE,
                             node_2_message.type)
            self.assertEqual(logical_plan_pb2.PbShuffleNode.BROADCAST,
                             node_2_message.shuffle_node.type)
            self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("2")),
                             entity.Entity.from_message(node_2_message.objector))
            self.assertFalse(node_2_message.shuffle_node.HasField("key_reader"))
            self._assert_belongs_to(node_2, scope)

    def test_shuffle_by_sequence(self):
        __operator = entity.Entity

        plan = self.plan

        start_0_1 = plan.load(["file1"]).by(test_helper.TestLoader())\
            .as_type(test_helper.TestObjector())
        node_0_1 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("0"))
        node_0_2 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("1"))
        node_0_3 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("2"))

        shuffle = plan.shuffle(plan.global_scope(), [node_0_1, node_0_2, node_0_3])\
            .with_concurrency(10)\
                .node(0).distribute_all()\
                .node(1).distribute_by_default()\
                .node(2).distribute_by(test_helper.TestPartitioner())\
            .done()

        self._finish_plan()

        self.assertEqual(7, len(self.message.node))
        self.assertEqual(3, len(self.message.scope))

        # check shuffle
        shuffle_message = self._proto_of(shuffle.scope())
        self.assertEqual(logical_plan_pb2.PbScope.BUCKET, shuffle_message.type)
        self.assertFalse(shuffle_message.is_sorted)
        self.assertTrue(shuffle_message.HasField("concurrency"))
        self.assertEqual(10, shuffle_message.concurrency)
        self.assertEqual(10, shuffle_message.bucket_scope.bucket_size)

        # check shuffle -- node 0
        node_0 = shuffle.node(0)
        node_0_message = self._proto_of(node_0)

        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE, node_0_message.type)
        self.assertEqual(logical_plan_pb2.PbShuffleNode.BROADCAST, node_0_message.shuffle_node.type)
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("0")),
                         entity.Entity.from_message(node_0_message.objector))

        self._assert_belongs_to(node_0, shuffle.scope())
        self._assert_from(node_0, node_0_1)

        # check shuffle -- node 1
        node_1 = shuffle.node(1)
        node_1_message = self._proto_of(node_1)

        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE, node_1_message.type)
        self.assertEqual(logical_plan_pb2.PbShuffleNode.SEQUENCE, node_1_message.shuffle_node.type)
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("1")),
                         entity.Entity.from_message(node_1_message.objector))
        self.assertFalse(node_1_message.shuffle_node.HasField("partitioner"))

        self._assert_belongs_to(node_1, shuffle.scope())
        self._assert_from(node_1, node_0_2)

        # check shuffle -- node 2
        node_2 = shuffle.node(2)
        node_2_message = self._proto_of(node_2)

        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE, node_2_message.type)
        self.assertEqual(logical_plan_pb2.PbShuffleNode.SEQUENCE, node_2_message.shuffle_node.type)
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("2")),
                         entity.Entity.from_message(node_2_message.objector))
        self.assertTrue(node_2_message.shuffle_node.HasField("partitioner"))
        self.assertEqual(entity.Entity.of(__operator.partitioner, test_helper.TestPartitioner()),
                         entity.Entity.from_message(node_2_message.shuffle_node.partitioner))

        self._assert_belongs_to(node_2, shuffle.scope())
        self._assert_from(node_2, node_0_3)

    def test_union(self):
        __operator = entity.Entity

        plan = self.plan

        source_1 = plan.load(["file1"]).by(test_helper.TestLoader())\
            .as_type(test_helper.TestObjector("u"))
        source_2 = plan.load(["file2"]).by(test_helper.TestLoader())\
            .as_type(test_helper.TestObjector("u"))
        start = plan.union(nodes=[source_1, source_2])
        node_1 = start.group_by(test_helper.TestKeyReader())
        node_2 = node_1.group_by(test_helper.TestKeyReader())
        node_3 = node_2.leave_scope()
        node_4 = node_2.remove_scope()

        self._finish_plan()

        # check start
        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.UNION_NODE, self._proto_of(start).type)
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("u")),
                         entity.Entity.from_message(self._proto_of(start).objector))
        self._assert_belongs_to(start, plan.global_scope())
        self._assert_from(start, source_1)
        self._assert_from(start, source_2)

        # check node 3/4
        checklist = [node_3, node_4]
        scopes = [node_1.scope(), plan.global_scope()]

        for i in range(0, 2):
            node = checklist[i]
            self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.UNION_NODE,
                             self._proto_of(node).type)
            self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("u")),
                             entity.Entity.from_message(self._proto_of(node).objector))
            self._assert_belongs_to(node, scopes[i])
            self._assert_from(node, node_2)

    def test_broadcast(self):
        __operator = entity.Entity

        plan = self.plan
        start_0_1 = plan.load(["file1"]).by(test_helper.TestLoader())\
            .as_type(test_helper.TestObjector())

        node_0_1 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("1"))
        node_0_2 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("2"))
        node_0_3 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("3"))

        shuffle_1 = plan.shuffle(start_0_1.scope(), [node_0_1, node_0_2, node_0_3])
        start_1_1 = shuffle_1.node(0).match_by(test_helper.TestKeyReader("1"))
        start_1_2 = shuffle_1.node(1).match_by(test_helper.TestKeyReader("2"))
        start_1_3 = shuffle_1.node(2).match_by(test_helper.TestKeyReader("3"))

        node_1_1 = start_1_1.sort_by(test_helper.TestKeyReader())  # implicit shuffle
        node_1_2 = start_1_2
        start_2_1 = start_1_3.group_by(test_helper.TestKeyReader())  # shuffle 2
        start_3_1 = start_1_3.sort_and_group_by(test_helper.TestKeyReader())  # shuffle 3

        node_1_4 = node_1_2.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("4"))

        shuffle_4 = plan.shuffle(shuffle_1.scope(), [node_1_1, node_1_2])\
            .node(0).match_by(test_helper.TestKeyReader())\
            .node(1).match_any()\
            .done()

        union_4 = plan.broadcast_to(node_0_3, shuffle_4.scope())
        union_1 = plan.broadcast_to(node_1_4, shuffle_1.scope())

        self._finish_plan()

        self.assertEqual(18, len(self.message.node))
        self.assertEqual(7, len(self.message.scope))

        # Check down-forward broadcast
        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.UNION_NODE,
                         self._proto_of(union_4).type)
        self._assert_belongs_to(union_4, shuffle_4.scope())

        # Check down-forward broadcast -- broadcast 1
        self.assertEqual(1, union_4.from_size())
        broadcast_1 = union_4.from_node(0)
        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE,
                         self._proto_of(broadcast_1).type)
        self.assertEqual(logical_plan_pb2.PbShuffleNode.BROADCAST,
                         self._proto_of(broadcast_1).shuffle_node.type)
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("3")),
                         entity.Entity.from_message(self._proto_of(broadcast_1).objector))
        self._assert_belongs_to(broadcast_1, shuffle_4.scope())

        # Check down-forward broadcast -- broadcast 2
        broadcast_2 = broadcast_1.from_node()
        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE,
                         self._proto_of(broadcast_2).type)
        self.assertEqual(logical_plan_pb2.PbShuffleNode.BROADCAST,
                         self._proto_of(broadcast_2).shuffle_node.type)
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("3")),
                         entity.Entity.from_message(self._proto_of(broadcast_2).objector))
        self._assert_belongs_to(broadcast_2, shuffle_1.scope())
        self._assert_from(broadcast_2, node_0_3)

        # Check same-level broadcast
        self.assertEqual(logical_plan_pb2.PbLogicalPlanNode.UNION_NODE,
                         self._proto_of(union_1).type)
        self.assertEqual(entity.Entity.of(__operator.objector, test_helper.TestObjector("4")),
                         entity.Entity.from_message(self._proto_of(union_1).objector))
        self._assert_belongs_to(union_1, shuffle_1.scope())
        self._assert_from(union_1, node_1_4)

    def test_upforward_broadcast(self):
        from bigflow import error

        plan = self.plan
        start_0_1 = plan.load(["file1"]).by(test_helper.TestLoader())\
            .as_type(test_helper.TestObjector())

        node_0_1 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("1"))
        node_0_2 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("2"))
        node_0_3 = start_0_1.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("3"))

        shuffle_1 = plan.shuffle(start_0_1.scope(), [node_0_1, node_0_2, node_0_3])
        start_1_1 = shuffle_1.node(0).match_by(test_helper.TestKeyReader("1"))
        start_1_2 = shuffle_1.node(1).match_by(test_helper.TestKeyReader("2"))

        node_1_1 = start_1_1.sort_by(test_helper.TestKeyReader())  # implicit shuffle
        node_1_2 = start_1_2
        node_1_4 = node_1_2.process_by(test_helper.TestProcessor())\
            .as_type(test_helper.TestObjector("4"))

        shuffle_4 = plan.shuffle(shuffle_1.scope(), [node_1_1, node_1_2])\
            .node(0).match_by(test_helper.TestKeyReader())\
            .node(1).match_any()\
            .done()

        self.assertRaisesRegexp(error.InvalidLogicalPlanException,
                                "Up-forward broadcasting is forbidden",
                                plan.broadcast_to,
                                node_1_4,
                                start_0_1.scope())
        self.assertRaisesRegexp(error.InvalidLogicalPlanException,
                                "Up-forward broadcasting is forbidden",
                                plan.broadcast_to,
                                node_1_4,
                                start_0_1.scope().father())
        self.assertRaisesRegexp(error.InvalidLogicalPlanException,
                                "Up-forward broadcasting is forbidden",
                                plan.broadcast_to,
                                shuffle_4.node(0),
                                start_0_1.scope())

    def test_processor_entities(self):
        from bigflow.core import entity_names
        acc = entity.Entity("doodle",
            entity.AccumulateProcessor(
                entity.PyFn(lambda:0),
                entity.CartesianFn())
        )
        msg = acc.to_proto_message()
        self.assertEqual(entity_names.__dict__["AccumulateProcessor"], msg.name)
        config = processor_pb2.PbPythonProcessorConfig()
        config.ParseFromString(msg.config)
        fn_names = []
        fn_config = []
        for config in config.functor:
            fn_names.append(config.name)
            fn_config.append(config.config)
        expect = ["PythonImplFunctor", "CartesianFn"]
        expect = map(lambda name: entity_names.__dict__[name], expect)
        self.assertEqual(expect, fn_names)

        self.assertEqual(False, cPickle.loads(fn_config[0])["expect_iterable"])
        self.assertEqual("", fn_config[1])

        flat_map = entity.Entity('doodle', entity.FlatMapProcessor(entity.PyFn(lambda:0)))
        msg = flat_map.to_proto_message()
        self.assertEqual(entity_names.__dict__["FlatMapProcessor"], msg.name)
        config = processor_pb2.PbPythonProcessorConfig()
        config.ParseFromString(msg.config)
        fns = []
        for config in config.functor:
            fns.append(config)
        self.assertEqual(1, len(fns))
        fn = fns[0]
        self.assertEqual(entity_names.__dict__["PythonImplFunctor"], fn.name)
        fn_config = cPickle.loads(fn.config)
        self.assertEqual(True, fn_config["expect_iterable"])

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
