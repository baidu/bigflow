#coding: utf-8

# Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import sys

_lazy_variable_id = 0
_first_time = {}
_var = {}
_self_module = sys.modules[__name__]

class LazyVariable(object):
    """ Lazy Variable，用于加载字典等外部数据

    Examples:

        >>> # coding: utf-8
        >>> from bigflow import base, lazy_var
        >>>
        >>> ""
        >>> mydict.txt
        >>>
        >>> key1    value1
        >>> key2    value2
        >>> key3    value3
        >>> ""
        >>>
        >>> def load_dict(path):
        >>>     data = dict()
        >>>     with open(path, "r") as f:
        >>>         for line in f:
        >>>             (key, value) = line.split()
        >>>             data[key] = value
        >>>     return data
        >>>
        >>> my_lazy_var = lazy_var.declare(lambda: load_dict("./mydict.txt"))
        >>>
        >>> def get_value(key):
        >>>     # 获取lazy_var内容
        >>>     my_dict = my_lazy_var.get()
        >>>     return my_dict.get(key)
        >>>
        >>> def main():
        >>>     my_dict = my_lazy_var.get()
        >>>     # 可以在本地直接获取lazy_var内容
        >>>     # ["value1", "value2", "value3"]
        >>>     print my_dict.values()
        >>>     pipeline = base.Pipeline.create("local")
        >>>     pipeline.add_file("./mydict.txt", "./mydict.txt")
        >>>     keys = pipeline.parallelize(["key1", "key2", "key3", "key4"])
        >>>     # 可以在transforms中获取lazy_var内容
        >>>     values = keys.map(get_value)
        >>>     # ["value1", "value2", "value3", None]
        >>>     print values.get()
        >>>
        >>> if __name__ == "__main__":
        >>>     main()

    """

    def __init__(self, var_id, fn):
        self.__var_id = var_id
        self.__fn = fn

    def get(self):
        """
        获取真正的Python Object，在第一次调用get方法时会构造一次Python Object并缓存住
        以后每次调用get直接从缓存中读取

        Args:
            None

        Returns:
            var(Python Object): 真正需要获取的Python Object

        Examples:
            >>> from bigflow import lazy_var
            >>> fn = lambda: {"A": "1", "B": "2", "C": "3"}
            >>>
            >>> my_lazy_var = lazy_var.declare(fn)
            >>> type(my_lazy_var)
            <class 'bigflow.lazy_var.LazyVariable'>
            >>>
            >>> my_dict = my_lazy_var.get()
            >>> my_dict
            {'A': '1', 'C': '3', 'B': '2'}
            >>> type(my_dict)
            <type 'dict'>
        """

        first_time = _self_module._first_time.get(self.__var_id, True)
        var = None

        if first_time:
            _self_module._first_time[self.__var_id] = False
            var = self.__fn()
            _self_module._var[self.__var_id] = var
        else:
            var = _self_module._var[self.__var_id]

        return var


def declare(fn):
    """
    根据给定的Python Object生成函数，生成一个 LazyVariable 变量；
    可以用该接口来加载一个外部字典或任意的Python Object
    在真正需要使用 Python Object 的时候，可以调用LazyVariable的get获取

    Args:
      fn(Python function or Python callable Object): 生成Python Object的函数

    Returns:
      lazy_variable(LazyVariable): 可被延迟加载的变量

    Examples:
        >>> from bigflow import lazy_var
        >>> fn = lambda: {"A": "1", "B": "2", "C": "3"}
        >>> my_lazy_var = lazy_var.declare(fn)
        >>> type(my_lazy_var)
        <class 'bigflow.lazy_var.LazyVariable'>
    """
    if not callable(fn) :
        raise ValueError("Invalid arguments: fn must be Python function or Python callable Object")

    var_id = _self_module._lazy_variable_id
    _self_module._lazy_variable_id += 1

    lazy_variable = LazyVariable(var_id, fn)
    return lazy_variable
