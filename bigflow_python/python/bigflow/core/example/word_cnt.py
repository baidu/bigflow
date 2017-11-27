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
A WordCount example written on Python LogicalPlan

"""
import sys
from bigflow.core.serde import record_objector
from bigflow import input, base, output
from bigflow.core import entity
from bigflow import serde

class PythonToRecordProcessor(entity.SelfNamedEntityBase):
        pass
class PythonFromRecordProcessor(entity.SelfNamedEntityBase):
        pass

class WordSpliter:
    def __init__(self):
        pass

    def begin(self, keys, inputs, emitter):
        self._emitter = emitter
    def process(self, index, record):
        words = record.split()
        for word in words:
            r = (word, 1)
            self._emitter.emit(r)
    def end(self):
        pass

class WordIdentity:
    def __init__(self, key_extractor, key_serde):
        self.objector = entity.Entity.of(entity.Entity.objector, key_serde) \
            .to_proto_message().SerializeToString()
        self.read_key = key_extractor

class WordCount:
    def __init__(self):
        pass

    def begin(self, keys, inputs, emitter):
        self._emitter = emitter
        self._sum = 0
        self._word = ""
    def process(self, index, record):
        if self._word == "" :
            self._word = record[0]
            self._sum = record[1]
        else:
            self._sum += record[1]
    def end(self):
        record = (self._word, self._sum)
        self._emitter.emit(record)


pipeline = base.Pipeline.create('local')
plan = pipeline.plan()
plan.set_environment(entity.PythonEnvironment())

input_path = sys.path[0] + "/" + __file__
input_urls = [input_path];
output_path = sys.path[0] + "/" + "output"

single_word = plan.load(input_urls)\
        .by(input.TextFile(input_urls[0]).input_format).as_type(record_objector.RecordObjector())\
        .process_by(PythonFromRecordProcessor()).as_type(serde.any())\
        .process_by(WordSpliter()).as_type(serde.any())

result = plan.shuffle(single_word.scope(), [single_word])\
        .with_concurrency(10)\
        .node(0).match_by(WordIdentity(lambda x: x[0], serde.any()))\
        .process_by(WordCount()).as_type(serde.any())\
        .input(0).allow_partial_processing().done()\
        .process_by(WordCount()).as_type(serde.any())

plan.shuffle(plan.global_scope(), [result]).node(0).distribute_by_default()\
        .process_by(PythonToRecordProcessor()).as_type(record_objector.RecordObjector())\
        .sink_by(output.TextFile(output_path).output_format)

pipeline.run()
