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
Base Flume Processor definitions, processor is a core operator of Bigflow backend("Flume" projct)
see `Flume Core document <http://bigflow.cloud/zh/flume/core.html>`_ for more details.

"""

from bigflow import error
from bigflow.util import ptype_info


class AbstractProcessor(object):
    """
    Base class of Processors

    Args:
      fn (function):  user-defined function, a callable Python object
    """

    def __init__(self, fn):
        self._fn = fn
        self._emitter = None

    def begin(self, keys, inputs, emitter):
        """
        Indicate the beginning of a group

        Args:
          keys (list):  keys of the group to start
          inputs (list):  re-iterable inputs with the group
          emitter (FlumeEmitter):  emitter to send the output
        """
        self._emitter = emitter
        pass

    def process(self, index, record):
        """
        Process the data

        Args:
          index (int):  indicating the index of current input
          record (object):  input data
        """
        pass

    def end(self):
        """
        Indicate the ending of a group
        """
        pass


class ProcessorWithSideInputs(AbstractProcessor):
    """
    An abstract Processor with SideInputs

    Args:
      fn (function):  user-defined function, a callable Python object
      side_inputs (PType):  SideInputs within the process
    """

    def __init__(self, fn, *side_inputs):
        super(ProcessorWithSideInputs, self).__init__(fn)

        self._side_input_types = map(lambda x: ptype_info.PTypeInfo(x), side_inputs)
        self._side_inputs = None

    def begin(self, keys, inputs, emitter):
        """
        Indicate the beginning of a group, SideInputs are wrapped from re-iterable inputs and
        are available as self._side_inputs

        Args:
          keys (list):  keys of the group to start
          inputs (list):  re-iterable inputs with the group
          emitter (FlumeEmitter):  emitter to send the output
        """
        super(ProcessorWithSideInputs, self).begin(keys, inputs, emitter)

        side_inputs = []
        assert len(self._side_input_types) == (len(inputs) - 1)

        for i in range(0, len(self._side_input_types)):
            if self._side_input_types[i].type == "PCollection":
                collection = []
                while inputs[i].has_next():
                    collection.append(inputs[i].next())

                side_inputs.append(collection)

            elif self._side_input_types[i].type == "PObject":
                if not inputs[i].has_next():
                    raise error.BigflowRuntimeException("PObject doesn't have any element")
                side_inputs.append(inputs[i].next())

        self._side_inputs = tuple(side_inputs)
