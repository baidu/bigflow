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
some magic method
Author: Zhang Yuncong (zhangyuncong@baidu.com)
"""

from bigflow import pobject
from bigflow import pcollection
from bigflow import ptable
from bigflow import ptype
from bigflow import transforms


class Col(object):
    def __init__(self, col_num):
        self.col_num = col_num

    def get(self, obj):
        return obj[self.col_num]

    def set(self, obj, val):
        ret = list(obj)
        ret[self.col_num] = val
        return type(obj)(ret)

    def __call__(self, obj):
        return self.get(obj)

class Field(object):
    def __init__(self, field_name):
        self.field_name = field_name

    def get(self, obj):
        return getattr(obj, self.field_name)

    def set(self, obj, val):
        setattr(obj, self.field_name, val)
        return obj

    def __call__(self, obj):
        return self.get(obj)

def col(num):
    return Col(num)

def field(field_name):
    return Field(field_name)

#def map_on(pcollection, field_extractor, fn):
#    return pcollection.map(lambda inp : field_extractor.set(inp, fn(field_extractor.get(inp))))

def on(field_extractor, fn):
    return lambda inp : field_extractor.set(inp, fn(field_extractor.get(inp)))

#p.apply(map, on(col(1), lambda x:x))

def map_on(pcollection, field_extractor, fn):
    return pcollection.map(on(field_extractor, fn))

def filter_on(pcollection, field_extractor, fn):
    return pcollection.filter(lambda inp : fn(field_extractor.get(inp)))

def flat_map_wrapper(inp, field_extractor, fn):
    return [field_extractor.set(inp, elem) for elem in fn(field_extractor.get(inp))]
    #for elem in fn(field_extractor.get(inp)):
    #    yield field_extractor.set(inp, elem)

def flat_map_on(pcollection, field_extractor, fn):
    return pcollection.flat_map(lambda inp : flat_map_wrapper(inp, field_extractor, fn))

def select(pcollection, *field_extractors):
    if len(field_extractors) == 1:
        return pcollection.map(lambda inp: field_extractors[0].get(inp))
    return pcollection.map(lambda inp: map(lambda extractor: extractor.get(inp), field_extractors))

def _if(cond_val, if_true, if_false):
    return transforms.union(if_true.filter(lambda _,v : v, cond_val), if_false.filter(lambda _,v:not v, cond_val))

def to_pcollection(obj):
    return obj.flat_map(lambda num: [num])

def convert_to(pcollection, to_type):
    return pcollection.map(lambda data: to_type(data))

def at(pcollection, n):
    def _map_result(o):
        assert len(o) == 2, "the pcollection has not enough elements"
        return o[1]
    if type(n) == pobject.PObject:
        return pcollection.accumulate((0, None, None), lambda o, i, n: (o[0] + 1, i) if (o[0] == n) else o, n) \
            .map(_map_result)
    else:
        return pcollection.accumulate((0, None, None), lambda o, i: (o[0] + 1, i) if (o[0] == n) else o) \
            .map(_map_result)

pcollection.PCollection.__getitem__ = at

def is_pobject_eq(pobject, expect):
    if isinstance(expect, ptype.PType):
        return pobject.map(lambda a, b: a == b, expect)
    else:
        return pobject.map(lambda a : a == expect)

pobject.PObject.__eq__ = is_pobject_eq

def fold(pcollection, zero, fn):
    return pcollection.aggregate(zero, fn, fn)

def and_all(pcollection):
    return pcollection.apply(fold, True, lambda a, b: a and b)

def is_unordered_eq(pcollection, expect):
    if not isinstance(expect, ptype.PType):
        expect = pcollection._pipeline.parallelize(expect)
    pcollection = pcollection.map(lambda elem: (elem, None))
    expect = expect.map(lambda elem: (elem, None))
    return pcollection.cogroup(expect).apply_values(lambda v1, v2: v1.count() == v2.count()) \
            .flatten_values() \
            .apply(and_all)

pcollection.PCollection.__eq__ = is_unordered_eq

def _ensure_ptype(a, b):
    from bigflow.util import broadcast
    if isinstance(b, ptype.PType):
        return b
    else:
        return broadcast.broadcast_to(a._pipeline.parallelize(b), a.node().scope())

padd = lambda a, b: a.map(lambda a, b: a + b, _ensure_ptype(a,b))
psub = lambda a, b: a.map(lambda a, b: a - b, _ensure_ptype(a,b))
pmul = lambda a, b: a.map(lambda a, b: a * b, _ensure_ptype(a,b))
pdiv = lambda a, b: a.map(lambda a, b: a / b, _ensure_ptype(a,b))

pobject.PObject.__add__ = padd
pcollection.PCollection.__add__ = padd


pobject.PObject.__sub__ = psub
pcollection.PCollection.__sub__ = psub

pobject.PObject.__mul__ = pmul
pcollection.PCollection.__mul__ = pmul

pobject.PObject.__div__ = pdiv
pcollection.PCollection.__div__ = pdiv

def passert_eq(a, b):
    def _check(val, a, b):
        assert val, "passert_eq failed: a = <%s>, b = <%s>" % (a, b)

    if not isinstance(a, ptype.PType) and not isinstance(b, ptype.PType):
        return a == b
    if isinstance(a, ptype.PType):
        (a == b).flat_map(lambda x: [x]).foreach(_check, a, _ensure_ptype(a, b))
    else:
        (a == b).flat_map(lambda x: [x]).foreach(_check, _ensure_ptype(b, a), b)

def passert(a):
    if not isinstance(a, ptype.PType):
        assert a

    def _check(val, a):
        assert val, "passert failed: condition = <%s>" % a

    a.foreach(_check, a)

def get(pcollection, n=None):
    """ get n elements """
    if n is None:
        return pcollection.get()
    else:
        pcollection.cache()
        return pcollection.take(n).get()


class Lambda(object):
    """ for val """

    def __getattr__(self, name):
        """ no comments """
        return lambda *args: (lambda p: getattr(p, name)(*args))

# Usage:
#   pipeline.parallelize({2:[1,2,3,4], 2:[1,2]}).apply_values(val.take(3)).get()
val = Lambda()
