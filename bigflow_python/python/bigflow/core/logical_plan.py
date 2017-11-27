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
API for creating logical execution plan. See http://project.baidu.com/flume/doc/core.html
for design considerations, see ****Proto file**** for data structure and serializations.

"""

import uuid

from bigflow import error
from bigflow.core import entity
from bigflow.util.log import logger
from flume.proto import logical_plan_pb2


class LogicalPlan(object):
    class Scope(object):
        def __init__(self, father, plan):
            self.__message = logical_plan_pb2.PbScope()
            self.__plan = plan
            self.__father = father

            if father is not None:
                self.__message.id = str(uuid.uuid4())
                self.__message.father = father.id()
            else:
                # Global scope
                self.__message.id = ""

        def id(self):
            return self.__message.id

        def father(self):
            return self.__father

        def is_sorted(self):
            return self.__message.is_sorted

        def is_infinite(self):
            """ return the is_infinite of scope """
            return self.__message.is_infinite

        def concurrency(self):
            return self.__message.concurrency

        @staticmethod
        def common_scope(inputs):

            def common_scope_of_scopes(scopes):
                stacks = []

                for scope in scopes:
                    stacks.append(scope.scope_stack())

                result = None

                while True:
                    top_scopes = []
                    for stack in stacks:
                        if len(stack) == 0:
                            if result is None:
                                raise error.InvalidLogicalPlanException("Error getting "
                                                                        "common scope")
                            return result

                        top_scopes.append(stack.pop())

                    count = top_scopes.count(top_scopes[0])
                    if count == len(top_scopes):
                        result = top_scopes[0]
                    else:
                        break

                if result is None:
                    raise error.InvalidLogicalPlanException("Error getting common scope")
                return result

            if len(inputs) == 0:
                raise ValueError("Invalid inputs")

            # for nodes
            if all(isinstance(node, LogicalPlan.Node) for node in inputs):
                return common_scope_of_scopes(map(lambda node: node.scope(), inputs))
            # for scopes
            else:
                if not all(isinstance(node, LogicalPlan.Scope) for node in inputs):
                    raise ValueError("Invalid inputs")
                return common_scope_of_scopes(inputs)

        def scope_stack(self):
            ancestors = []
            scope = self
            while scope:
                ancestors.append(scope)
                scope = scope.father()

            return ancestors

        def is_cover_by(self, scope):
            ancestor = self
            while ancestor is not None and ancestor is not scope:
                ancestor = ancestor.father()

            return ancestor is scope

        def to_proto_message(self):
            if self.__message.type == logical_plan_pb2.PbScope.BUCKET \
                    and self.__message.HasField("concurrency"):
                self.__message.bucket_scope.bucket_size = self.__message.concurrency

            return self.__message

        def __str__(self):
            return "Scope: %s" % self.id()

    class Node(object):
        def __init__(self, node_type, scope, plan):
            self._node_type = node_type
            self._type_str = "RawNode"
            self._scope = scope
            self._plan = plan
            self._id = str(uuid.uuid4())
            self._cache = False
            self._objector_entity = None
            self._debug_info = ""
            self._serde = None
            self._extra_debug_info = ""
            self._size = 0
            self._memory_limit = -1
            self._cpu_limit = -1
            self._is_infinite = False

        def serde(self):
            """ return the serde who is used to create this node
            """
            return self._serde

        def id(self):
            return self._id

        def type(self):
            return self._node_type

        def scope(self):
            return self._scope

        def plan(self):
            return self._plan

        def debug_info(self):
            return self._debug_info

        def set_debug_info(self, debug_info):
            self._debug_info = debug_info
            return self

        def objector(self):
            return self._objector_entity

        def set_objector(self, value):
            self._objector_entity = value

        def cache(self):
            self._cache = True

        def size(self):
            """
            Data size of this node
            """
            return self._size

        def set_size(self, size=None, scale_factor=1.0):
            """
            Set data size of this node
            """
            if size is not None:
                self._size = size
            return self

        def set_memory(self, memory_limit):
            """
            Set memory limit for this node
            """
            self._memory_limit = memory_limit
            return self

        def set_cpu(self, cpu_limit):
            """
            Set cpu limit for this node
            """
            self._cpu_limit = int(cpu_limit * 100)
            return self

        def is_infinite(self):
            """
            Infinite/Finite of this node
            """
            return self._is_infinite

        def set_infinite(self):
            """
            Set infinite for this node
            """
            self._is_infinite = True
            return self

        @staticmethod
        def if_infinite(from_nodes, target_scope):
            """
            Get infinte of from_nodes in the scope
            """
            result = False
            for from_node in from_nodes:
                if from_node.scope() is target_scope:
                    result = result or from_node.is_infinite()
                else:
                    scope = from_node.scope()
                    while True:
                        if scope.father() is target_scope:
                            result = result or scope.is_infinite()
                            break
                        elif scope.father() is None:
                            raise ValueError("Invalid from_nodes")
                        scope = scope.father()
            return result

        def leave_scope(self):
            if self._scope.father() is None:
                raise error.InvalidLogicalPlanException("Trying to leave global scope")

            union_node = self._plan.union(self._scope.father(), [self])
            union_node.set_objector(self._objector_entity)

            return union_node

        def remove_scope(self):
            union_node = self._plan.union(self._plan.global_scope(), [self])
            union_node.set_objector(self._objector_entity)

            return union_node

        def sink_by(self, sink_obj):
            return self._plan.sink(self._scope, self).by(sink_obj)

        def process_by(self, process_obj):
            return self._plan.process(from_nodes=[self]).by(process_obj)

        def combine_with(self, node):
            return self._plan.process(scope=None, from_nodes=[self, node])

        def sort_by(self, key_reader_obj):
            return self._plan.shuffle(self._scope, from_nodes=[self])\
                .sort()\
                .node(0).match_by(key_reader_obj, entity.Entity.sort_key_reader)\
                .set_debug_info("SortBy: " + repr(key_reader_obj)) \
                .leave_scope()

        def group_by(self, key_reader_obj):
            shuffle_group = self._plan.shuffle(self._scope, from_nodes=[self])
            return shuffle_group.node(0).match_by(key_reader_obj) \
                .set_debug_info("GroupBy: " + repr(key_reader_obj))

        def sort_and_group_by(self, key_reader_obj):
            return self._plan.shuffle(self._scope, from_nodes=[self]).sort()\
                .node(0).match_by(key_reader_obj) \
                .set_debug_info("SortAndGroupBy: " + repr(key_reader_obj))

        def distribute_into(self, bucket_number):
            return self._plan.shuffle(self._scope, from_nodes=[self])\
                .with_concurrency(bucket_number)\
                .node(0).distribute_by_default()

        def append_debug_info(self, extra_info):
            """
            this debug infos will be appended at the end of the debug info
            """
            self._extra_debug_info += extra_info

        def to_proto_message(self):
            message = logical_plan_pb2.PbLogicalPlanNode()

            message.id = self._id
            message.type = self._node_type
            message.debug_info = \
                self._debug_info + \
                self._extra_debug_info + \
                ", size: " + str(self._size)
            message.cache = self._cache
            if self._memory_limit != -1:
                message.memory_limit = self._memory_limit
            if self._cpu_limit != -1:
                message.cpu_limit = self._cpu_limit
            message.is_infinite = self._is_infinite

            if self._objector_entity is None \
                    and self._node_type != logical_plan_pb2.PbLogicalPlanNode.SINK_NODE:
                raise error.InvalidLogicalPlanException("Non sink node must have objector!")

            if self._objector_entity is not None:
                message.objector.CopyFrom(self._objector_entity.to_proto_message())

            message.scope = self._scope.id()
            self.set_specific_field(message)

            return message

        def __str__(self):
            return "%s: %s" % (self._type_str, self._id)

    class _ShuffleGroup(object):
        def __init__(self, scope):
            self.__scope = scope
            self.__nodes = []

        def scope(self):
            return self.__scope

        def id(self):
            return self.__scope.id()

        def node_size(self):
            return len(self.__nodes)

        def node(self, index):
            return self.__nodes[index]

        def sort(self):
            self.__scope._Scope__message.is_sorted = True
            return self

        def with_concurrency(self, concurrency):
            if isinstance(concurrency, float):
                concurrency = int(concurrency)
            if concurrency < 1:
                concurrency = 1

            self.__scope._Scope__message.concurrency = concurrency
            return self

        def window_by(self, window, trigger=None):
            scope = self.__scope._Scope__message
            scope.type = logical_plan_pb2.PbScope.WINDOW
            scope.window_scope.window \
                .CopyFrom(entity.Entity.of(entity.Entity.window, window).to_proto_message())
            if trigger is not None:
                scope.window_scope.trigger \
                    .CopyFrom(entity.Entity.of(entity.Entity.trigger, trigger).to_proto_message())

            return self

        def __nodes_is_infinite(self, nodes):
            return any(node.is_infinite() for node in nodes)

        def __add_node(self, node):
            self.__nodes.append(node)

        def __str__(self):
            return "ShuffleGroup: %s" % self.id()

    class UnionNode(Node):
        def __init__(self, from_nodes, scope, plan):
            LogicalPlan.Node.__init__(self,
                                      logical_plan_pb2.PbLogicalPlanNode.UNION_NODE,
                                      scope,
                                      plan)
            self._type_str = "UnionNode"
            self.__from_nodes = from_nodes
            self._size = sum(map(lambda x: x.size(), self.__from_nodes))
            objector = None
            for from_node in from_nodes:
                assert from_node._serde is not None
                self._serde = from_node._serde

                if from_node.objector() is None:
                    raise error.InvalidLogicalPlanException("Error getting objector from inputs")

                if objector is None:
                    objector = from_node.objector()
                elif objector != from_node.objector():
                    raise error.InvalidLogicalPlanException("Union sources with different objectors"
                                                            ", user must set objector manually")

            self.set_objector(objector)

        def from_size(self):
            return len(self.__from_nodes)

        def from_node(self, index):
            return self.__from_nodes[index]

        # override
        def set_size(self, size=None, scale_factor=1.0):
            """
            Set data size of this node
            """
            logger.warning("Trying to set size for UnionNode, ignore")
            return self

        def set_specific_field(self, message):
            for from_node in self.__from_nodes:
                message.union_node.__getattribute__("from").append(from_node.id())

    class LoadNode(Node):
        def __init__(self, uri_list, scope, plan):
            LogicalPlan.Node.__init__(self,
                                      logical_plan_pb2.PbLogicalPlanNode.LOAD_NODE,
                                      scope,
                                      plan)
            self._type_str = "LoadNode"
            self.__uri_list = uri_list
            self.__loader_entity = None

        def repeatedly(self):
            """ set load node is infinite """
            self._scope._Scope__message.is_infinite = True
            self.set_infinite()
            return self

        def uri_size(self):
            return len(self.__uri_list)

        def uri(self, index):
            return self.__uri_list[index]

        def set_loader(self, loader):
            self.__loader_entity = loader

        def loader(self):
            return self.__loader_entity

        def by(self, loader_obj):
            loader_entity = entity.Entity.of(entity.Entity.loader, loader_obj)
            self.set_loader(loader_entity)

            scope_message = self._scope._Scope__message
            if scope_message.type != logical_plan_pb2.PbScope.INPUT:
                raise error.InvalidLogicalPlanException("Invalid message")

            scope_message.input_scope.spliter.CopyFrom(loader_entity.to_proto_message())
            return self

        def as_type(self, objector_obj):
            from bigflow import serde
            assert isinstance(objector_obj, serde.Serde), type(objector_obj)
            self._serde = objector_obj
            objector = entity.Entity(entity.Entity.objector, objector_obj)
            self.set_objector(objector)
            self.append_debug_info("\nserde: " + str(objector_obj))
            return self

        # override
        def set_size(self, size=None, scale_factor=1.0):
            """
            Set data size of this node
            """
            if size is None:
                raise error.BigflowPlanningException("Empty input size for loader")
            return super(LogicalPlan.LoadNode, self).set_size(size, scale_factor)

        def set_specific_field(self, message):
            if self.__loader_entity is None:
                raise error.InvalidLogicalPlanException("Invalid loader")

            for uri in self.__uri_list:
                message.load_node.uri.append(uri)

            message.load_node.loader.CopyFrom(self.__loader_entity.to_proto_message())

    class SinkNode(Node):
        def __init__(self, from_node, scope, plan):
            LogicalPlan.Node.__init__(self,
                                      logical_plan_pb2.PbLogicalPlanNode.SINK_NODE,
                                      scope,
                                      plan)
            self._type_str = "SinkNode"
            self.__from_node = from_node
            self.__sinker_entity = None

        def set_specific_field(self, message):
            if self.__sinker_entity is None:
                raise error.InvalidLogicalPlanException("Invalid sinker")

            sink_message = logical_plan_pb2.PbSinkNode()
            sink_message.__setattr__("from", self.__from_node.id())
            sink_message.sinker.CopyFrom(self.__sinker_entity.to_proto_message())

            message.sink_node.CopyFrom(sink_message)

        def set_sinker(self, sinker_entity):
            self.__sinker_entity = sinker_entity

        def sinker(self):
            return self.__sinker_entity

        def by(self, sinker_obj):
            sinker = entity.Entity(entity.Entity.sinker, sinker_obj)
            self.set_sinker(sinker)

            return self

        # override
        def set_size(self, size=None, scale_factor=1.0):
            """
            Set data size of this node
            """
            if scale_factor != 1.0:
                logger.warning("Trying to set scale factor for ShuffleNode, ignore")
            self._size = self.__from_node.size()
            return super(LogicalPlan.SinkNode, self).set_size(size, scale_factor)

        def _type_str(self):
            return "SinkNode"

    class ProcessNode(Node):

        class Input(object):
            def __init__(self, from_node, process_node):
                self.from_node = from_node
                self.process_node = process_node
                self.is_partial = False
                self.is_prepared = False

            def done(self):
                return self.process_node

            def allow_partial_processing(self):
                self.is_partial = True
                return self

            def prepare_before_processing(self):
                self.is_prepared = True
                return self

            def input_size(self):
                return self.process_node.input_size()

            def input(self, index):
                return self.process_node.input(index)

            def to_proto_message(self):
                message = logical_plan_pb2.PbProcessNode.Input()
                message.__setattr__("from", self.from_node.id())
                message.is_partial = self.is_partial
                message.is_prepared = self.is_prepared
                return message

        def __init__(self, from_nodes, scope, plan):
            LogicalPlan.Node.__init__(self,
                                      logical_plan_pb2.PbLogicalPlanNode.PROCESS_NODE,
                                      scope,
                                      plan)
            self._type_str = "ProcessNode"
            self.__least_prepared_inputs = 0
            self.__effective_key_num = -1
            self.__is_stateful = False
            self.__is_ignore_group = False
            self.__allow_massive_instance = False
            self.__processor_entity = None
            self.__inputs = map(lambda x: LogicalPlan.ProcessNode.Input(x, self), from_nodes)
            self._size = sum(map(lambda x: x.from_node.size(), self.__inputs)) * 0.9

        def processor(self):
            return self.__processor_entity

        def least_prepared_inputs(self):
            return self.__least_prepared_inputs

        def is_ignore_group(self):
            return self.__is_ignore_group

        def effective_key_num(self):
            return self.__effective_key_num

        def is_stateful(self):
            """
            ProcessNode is stateful
            """
            self.__is_stateful = True
            return self

        def allow_massive_instance(self):
            self.__allow_massive_instance = True
            return self

        def ignore_group(self):
            self.__is_ignore_group = True
            return self

        def prepare_at_least(self, number):
            self.__least_prepared_inputs = number
            return self

        def set_effective_key_num(self, number):
            self.__effective_key_num = number
            return self

        def input_size(self):
            return len(self.__inputs)

        def input(self, index):
            return self.__inputs[index]

        def set_size(self, size=None, scale_factor=1.0):
            """
            Set data size of this node
            """
            self._size = sum(map(lambda x: x.from_node.size(), self.__inputs)) * scale_factor
            return super(LogicalPlan.ProcessNode, self).set_size(size, scale_factor)

        def set_specific_field(self, message):
            if self.__processor_entity is None:
                raise error.InvalidLogicalPlanException("Invalid processor")

            pb_process_node = message.process_node

            pb_process_node.processor.CopyFrom(self.__processor_entity.to_proto_message())
            pb_process_node.least_prepared_inputs = self.__least_prepared_inputs
            pb_process_node.is_ignore_group = self.__is_ignore_group
            pb_process_node.is_stateful = self.__is_stateful
            pb_process_node.effective_key_num = self.__effective_key_num

            for _input in self.__inputs:
                input_message = pb_process_node.input.add()
                input_message.CopyFrom(_input.to_proto_message())

        def by(self, processor_obj):
            processor = entity.Entity.of(entity.Entity.processor, processor_obj)
            self.__processor_entity = processor
            return self

        def as_type(self, objector_obj):
            self._serde = objector_obj
            objector = entity.Entity(entity.Entity.objector, objector_obj)
            self.set_objector(objector)
            self.append_debug_info("\nserde: " + str(objector_obj))

            return self

    class ShuffleNode(Node):
        def __init__(self, from_node, group, plan):
            LogicalPlan.Node.__init__(self,
                                      logical_plan_pb2.PbLogicalPlanNode.SHUFFLE_NODE,
                                      group.scope(),
                                      plan)
            self._type_str = "ShuffleNode"
            self.__from_node = from_node
            self.__group = group
            self.__group._ShuffleGroup__add_node(self)
            self.__shuffle_type = logical_plan_pb2.PbShuffleNode.BROADCAST
            self.__key_reader = None
            self.__partitioner = None
            self.__window = None
            self.set_objector(from_node.objector())
            self._serde = from_node._serde
            self._size = self.__from_node.size()

        def from_node(self):
            return self.__from_node

        def shuffle_type(self):
            return self.__shuffle_type

        def set_shuffle_type(self, value):
            self.__shuffle_type = value

        def key_reader(self):
            return self.__key_reader

        def set_key_reader(self, value):
            self.__key_reader = value

        def partitioner(self):
            return self.__partitioner

        def set_partitioner(self, value):
            self.__partitioner = value

        def set_time_reader(self, value):
            self.__time_reader = value

        def time_by(self, time_reader):
            if self.__from_node.is_infinite():
                self.__group.scope()._Scope__message.is_infinite = True
                self.__group.scope()._Scope__message.is_stream = False
            self.__set_scope_type(logical_plan_pb2.PbScope.WINDOW)
            self.set_shuffle_type(logical_plan_pb2.PbShuffleNode.WINDOW)
            ent = entity.Entity.of(entity.Entity.time_reader, time_reader)
            self.set_time_reader(ent)
            return self

        def node_size(self):
            return self.__group.node_size()

        def node(self, index):
            return self.__group.node(index)

        def done(self):
            return self.__group

        def match_any(self):
            self.__set_scope_type(logical_plan_pb2.PbScope.GROUP)
            self.set_shuffle_type(logical_plan_pb2.PbShuffleNode.BROADCAST)
            return self

        def broadcast(self):
            self.set_shuffle_type(logical_plan_pb2.PbShuffleNode.BROADCAST)
            return self

        def match_by(self, key_reader_obj, key_reader=entity.Entity.key_reader):
            self.__set_scope_type(logical_plan_pb2.PbScope.GROUP)
            self.set_shuffle_type(logical_plan_pb2.PbShuffleNode.KEY)
            self.set_key_reader(entity.Entity.of(key_reader, key_reader_obj))
            if self.__from_node.is_infinite():
                self.set_infinite()
                self.__group.scope()._Scope__message.is_infinite = True
                self.__group.scope()._Scope__message.is_stream = True
            return self

        def distribute_all(self):
            self.__set_scope_type(logical_plan_pb2.PbScope.BUCKET)
            self.set_shuffle_type(logical_plan_pb2.PbShuffleNode.BROADCAST)
            return self

        def distribute_by(self, partitioner_obj):
            self.__set_scope_type(logical_plan_pb2.PbScope.BUCKET)
            self.set_shuffle_type(logical_plan_pb2.PbShuffleNode.SEQUENCE)
            self.set_partitioner(
                entity.Entity.of(entity.Entity.partitioner, partitioner_obj)
            )
            if self.__from_node.is_infinite():
                self.set_infinite()
                self.__group.scope()._Scope__message.is_infinite = True
                self.__group.scope()._Scope__message.is_stream = True
            return self

        def distribute_by_default(self):
            self.__set_scope_type(logical_plan_pb2.PbScope.BUCKET)
            self.set_shuffle_type(logical_plan_pb2.PbShuffleNode.SEQUENCE)
            if self.__from_node.is_infinite():
                self.set_infinite()
                self.__group.scope()._Scope__message.is_infinite = True
                self.__group.scope()._Scope__message.is_stream = True
            return self

        def distribute_every(self):
            """ distribute by every record """
            self.__set_scope_type(logical_plan_pb2.PbScope.BUCKET)
            self.set_shuffle_type(logical_plan_pb2.PbShuffleNode.SEQUENCE)
            self.__set_distribute_every(True)
            return self

        def distribute_as_batch(self):
            """ distribute as batch """
            self.__set_scope_type(logical_plan_pb2.PbScope.BUCKET)
            self.set_shuffle_type(logical_plan_pb2.PbShuffleNode.SEQUENCE)
            if self.__from_node.is_infinite():
                self.__group.scope()._Scope__message.is_infinite = True
            return self

        # override
        def set_size(self, size=None, scale_factor=1.0):
            """
            Set data size of this node
            """
            if scale_factor != 1.0:
                logger.warning("Trying to set scale factor for ShuffleNode, ignore")
            self._size = self.__from_node.size()
            return super(LogicalPlan.ShuffleNode, self).set_size(size, scale_factor)

        def __set_scope_type(self, scope_type):
            scope_message = self._scope._Scope__message

            if scope_message.type != logical_plan_pb2.PbScope.DEFAULT \
                and scope_message.type != scope_type:
                raise error.InvalidLogicalPlanException("Invalid scope type")

            scope_message.type = scope_type

        def __set_distribute_every(self, distribute_every=True):
            scope_message = self._scope._Scope__message
            scope_message.distribute_every = distribute_every

        def set_specific_field(self, message):
            shuffle_node = message.shuffle_node
            shuffle_node.__setattr__("from", self.__from_node.id())
            shuffle_node.type = self.__shuffle_type

            if self.__shuffle_type == logical_plan_pb2.PbShuffleNode.KEY:
                if self.__key_reader is None:
                    raise error.InvalidLogicalPlanException("Invalid key reader")

                shuffle_node.key_reader.CopyFrom(self.__key_reader.to_proto_message())

            if self.__shuffle_type == logical_plan_pb2.PbShuffleNode.SEQUENCE \
                    and self.__partitioner is not None:
                shuffle_node.partitioner.CopyFrom(self.__partitioner.to_proto_message())

            if self.__shuffle_type == logical_plan_pb2.PbShuffleNode.WINDOW \
                    and self.__time_reader is not None:
                shuffle_node.time_reader.CopyFrom(self.__time_reader.to_proto_message())

    class Status(object):
        def __init__(self):
            self.ok = True
            self.reason = None

    def __init__(self):
        self.__scopes = [LogicalPlan.Scope(None, self)]
        self.__nodes = []
        self.__shuffles = []
        self._environment = None

    def node_size(self):
        return len(self.__nodes)

    def node(self, index):
        return self.__nodes[index]

    def scope_size(self):
        return len(self.__scopes)

    def scope(self, index):
        return self.__scopes[index]

    def global_scope(self):
        return self.__scopes[0]

    def shuffle_size(self):
        return len(self.__shuffles)

    def shuffle_group(self, index):
        return self.__shuffle[index]

    def load(self, uris):
        """ create load node """
        scope = LogicalPlan.Scope(self.global_scope(), self)
        self.__scopes.append(scope)

        scope_message = scope._Scope__message
        scope_message.type = logical_plan_pb2.PbScope.INPUT

        input_scope = scope_message.input_scope
        input_scope.uri.extend(uris)

        load_node = LogicalPlan.LoadNode(uris, scope, self)
        self.__nodes.append(load_node)

        return load_node

    def sink(self, scope, from_node):
        if from_node.type() == logical_plan_pb2.PbLogicalPlanNode.SINK_NODE:
            raise error.InvalidLogicalPlanException("Invalid plan: sinker's user cannot be sinker")

        if not from_node.scope().is_cover_by(scope):
            raise error.InvalidLogicalPlanException("Sinker must be in right scope")

        sink_node = LogicalPlan.SinkNode(from_node, scope, self)
        if LogicalPlan.Node.if_infinite([from_node], scope):
            sink_node.set_infinite()
        self.__nodes.append(sink_node)

        return sink_node

    def process(self, scope=None, from_nodes=None):
        if from_nodes is None:
            from_nodes = []

        if not isinstance(from_nodes, list) or len(from_nodes) == 0:
            raise error.InvalidLogicalPlanException("Invalid arguments: "
                                                    "from_nodes must be valid list")

        def process_with_scope(_scope):
            if not all(isinstance(node, LogicalPlan.Node) for node in from_nodes) \
                    or not all(node.scope().is_cover_by(_scope) for node in from_nodes):
                raise error.InvalidLogicalPlanException("Invalid arguments: wrong processed nodes")

            process_node = LogicalPlan.ProcessNode(from_nodes, _scope, self)
            if LogicalPlan.Node.if_infinite(from_nodes, _scope):
                process_node.set_infinite()
            self.__nodes.append(process_node)

            return process_node

        if scope is not None:
            return process_with_scope(scope)
        else:
            common_scope = LogicalPlan.Scope.common_scope(from_nodes)
            return process_with_scope(common_scope)

    def union(self, scope=None, nodes=None):
        if nodes is None:
            nodes = []

        if not isinstance(nodes, list) or len(nodes) == 0:
            raise error.InvalidLogicalPlanException("Invalid arguments: "
                                                    "nodes to union must be valid list")

        def union_with_scope(_scope):
            if not all(isinstance(node, LogicalPlan.Node) for node in nodes) \
                    or not all(node.scope().is_cover_by(_scope) for node in nodes):
                raise error.InvalidLogicalPlanException("Invalid arguments: wrong nodes to union")

            union_node = LogicalPlan.UnionNode(nodes, _scope, self)
            if LogicalPlan.Node.if_infinite(nodes, _scope):
                union_node.set_infinite()
            self.__nodes.append(union_node)
            return union_node

        if scope is not None:
            return union_with_scope(scope)
        else:
            common_scope = LogicalPlan.Scope.common_scope(nodes)
            return union_with_scope(common_scope)

    def shuffle(self, scope, from_nodes):
        if not isinstance(from_nodes, list) or len(from_nodes) == 0:
            raise error.InvalidLogicalPlanException("Invalid arguments: "
                                                    "nodes to shuffle must be valid list")

        shuffle_scope = LogicalPlan.Scope(scope, self)
        self.__scopes.append(shuffle_scope)

        shuffle_group = LogicalPlan._ShuffleGroup(shuffle_scope)
        self.__shuffles.append(shuffle_group)

        for from_node in from_nodes:
            shuffle_node = LogicalPlan.ShuffleNode(from_node, shuffle_group, self)
            self.__nodes.append(shuffle_node)

        return shuffle_group

    def broadcast_to(self, source_node, target_scope):
        common_scope = LogicalPlan.Scope.common_scope([source_node.scope(), target_scope])

        if target_scope is common_scope and target_scope is not source_node.scope():
            raise error.InvalidLogicalPlanException("Up-forward broadcasting is forbidden")

        scopes = []
        scope = target_scope
        while scope is not None and scope is not common_scope:
            scopes.append(scope)
            scope = scope.father()

        current_node = source_node
        for scope in reversed(scopes):
            current_node = self.__add_shuffle_node(current_node, scope)

        union_node = LogicalPlan.UnionNode([current_node], target_scope, self)
        self.__nodes.append(union_node)

        return union_node

    def __add_shuffle_node(self, source_node, target_scope):
        if target_scope.father() is not source_node.scope():
            raise error.InvalidLogicalPlanException("Source node should only belong to"
                                                    " target scope's father")

        # Find ShuffleGroup for Scope
        shuffle_group = None
        for shuffle in self.__shuffles:
            if shuffle.scope() is target_scope:
                shuffle_group = shuffle
                break

        if shuffle_group is None:
            raise error.InvalidLogicalPlanException("Unable to find corresponding "
                                                    "Shuffle Group for target scope")

        shuffle_node = LogicalPlan.ShuffleNode(source_node, shuffle_group, self).broadcast()
        self.__nodes.append(shuffle_node)

        return shuffle_node

    def set_environment(self, env):
        self._environment = env

    def to_proto_message(self):
        message = logical_plan_pb2.PbLogicalPlan()

        for node in self.__nodes:
            node_message = message.node.add()
            node_message.CopyFrom(node.to_proto_message())

        for scope in self.__scopes:
            scope_message = message.scope.add()
            scope_message.CopyFrom(scope.to_proto_message())

        if self._environment:
            message.environment.CopyFrom(entity.Entity.of("EntitiedBySelf", self._environment).to_proto_message())

        if not message.IsInitialized():
            raise error.InvalidLogicalPlanException("Message is not initialized")

        return message


if __name__ == "__main__":
    pass
