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
Helper class for LogicalPlan unit tests

"""

from flume.proto import logical_plan_pb2

from bigflow import serde

class TestLoader(object):
    def __init__(self, test_info=""):
        self.__test_info = test_info

    def setup(self, config):
        pass

    def split(self, uri, splits):
        pass

    def load(self, split, emitter):
        pass

    def __eq__(self, other):
        if isinstance(other, TestLoader):
            return self.__test_info == other.__test_info
        return False

    def __ne__(self, other):
        return not self == other


class TestObjector(serde.Serde):
    def __init__(self, test_info=""):
        self.__test_info = test_info

    def setup(self, config):
        pass

    def serialize(self):
        return ""

    def deserialize(self, bytes):
        pass

    def __eq__(self, other):
        if isinstance(other, TestObjector):
            return self.__test_info == other.__test_info
        return False

    def __ne__(self, other):
        return not self == other


class TestSinker(object):
    def __init__(self, test_info=""):
        self.__test_info = test_info

    def setup(self, config):
        pass

    def open(self, keys):
        pass

    def sink(self, obj):
        pass

    def close(self):
        pass

    def __eq__(self, other):
        if isinstance(other, TestSinker):
            return self.__test_info == other.__test_info
        return False

    def __ne__(self, other):
        return not self == other


class TestProcessor(object):
    def __init__(self, test_info=""):
        self.__test_info = test_info

    def setup(self, config):
        pass

    def begin(self, keys, iterators):
        pass

    def process(self, index):
        pass

    def end(self):
        pass

    def __eq__(self, other):
        if isinstance(other, TestProcessor):
            return self.__test_info == other.__test_info
        return False

    def __ne__(self, other):
        return not self == other


class TestKeyReader(object):
    def __init__(self, test_info=""):
        self.__test_info = test_info

    def setup(self, config):
        pass

    def begin(self, keys, iterators):
        pass

    def process(self, index):
        pass

    def end(self):
        pass

    def __eq__(self, other):
        if isinstance(other, TestKeyReader):
            return self.__test_info == other.__test_info
        return False

    def __ne__(self, other):
        return not self == other


class TestPartitioner(object):
    def __init__(self, test_info=""):
        self.__test_info = test_info

    def setup(self, config):
        pass

    def begin(self, keys, iterators):
        pass

    def process(self, index):
        pass

    def end(self):
        pass

    def __eq__(self, other):
        if isinstance(other, TestPartitioner):
            return self.__test_info == other.__test_info
        return False

    def __ne__(self, other):
        return not self == other


class Vertex(object):
    def __init__(self, identity):
        self.id = identity
        self.father = None
        self.froms = dict()

    def add_from(self, from_vertex):
        self.froms[from_vertex.id] = from_vertex

    def add_father(self, father):
        self.father = father

    def __eq__(self, other):
        if isinstance(other, Vertex):
            return self.id == other.id
        return False

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.id)


def find_or_insert_vertex(node_id, vertices):
    if node_id in vertices:
        result = vertices[node_id]
    else:
        result = Vertex(node_id)
        vertices[node_id] = result

    return result


def build_dag_tree(message, dag_tree, vertices):
    """
    :param message: Protobuf message of LogicalPlan
    :param dag_tree: DAG
    :param vertices: All vertices built from LogicalPlan message
    :return: True if successful, false otherwise
    """

    def parse_dependency():
        if node_message.type == logical_plan_pb2.PbLogicalPlanNode.UNION_NODE:
            for from_node_id in node_message.union_node.__getattribute__('from'):
                node.add_from(find_or_insert_vertex(from_node_id, vertices))

        elif node_message.type == logical_plan_pb2.PbLogicalPlanNode.SINK_NODE:
            from_node_id = node_message.sink_node.__getattribute__('from')
            node.add_from(find_or_insert_vertex(from_node_id, vertices))

        elif node_message.type == logical_plan_pb2.PbLogicalPlanNode.PROCESS_NODE:
            for from_node in node_message.process_node.input:
                from_node_id = from_node.__getattribute__('from')
                node.add_from(find_or_insert_vertex(from_node_id, vertices))

        elif node_message.type == logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE:
            from_node_id = node_message.shuffle_node.__getattribute__('from')
            node.add_from(find_or_insert_vertex(from_node_id, vertices))
        else:
            # LOAD_NODE
            pass

    for scope_message in message.scope:
        scope = find_or_insert_vertex(scope_message.id, vertices)
        dag_tree[scope] = scope_message

        if scope_message.HasField('father'):
            father = find_or_insert_vertex(scope_message.father, vertices)
            scope.add_father(father)

    for node_message in message.node:
        node = find_or_insert_vertex(node_message.id, vertices)
        dag_tree[node] = node_message

        scope = find_or_insert_vertex(node_message.scope, vertices)
        node.add_father(scope)

        parse_dependency()

    return True
