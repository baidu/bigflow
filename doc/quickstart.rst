快速入门
========

* `在Python shell中使用Bigflow Python`_

  * `准备`_
  * `更多的变换操作`_

* `完整的程序`_

  * `批处理引擎（spark）`_

* `进阶教程`_

本教程旨在提供一个使用Bigflow Python的快速上手演示。教程首先会通过在Python交互式环境(Interactive Shell)中一步一步展示Bigflow Python的基本概念和API，然后演示如何写一个完整的Bigflow Python程序。对于深入的概念和用法，请参照 :doc:`编程指南 <guide>` 部分。

在Python shell中使用Bigflow Python
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

准备
####

Bigflow Python工作在Python 2.7环境中(其它版本尚未测试)。它使用标准的CPython解释器，同时用户也能够在代码中使用第三方的Python库。

获取 `bigflow_python.tar.gz <https://github.com/baidu/bigflow/releases>`_


保证系统环境中已经安装好hadoop client，并设置好HADOOP_HOME，Bigflow Python需要从Hadoop配置文件中读取相应的配置来访问HDFS。
保证系统环境中已经安装好spark client，并设置好SPARK_HOME，Bigflow Python需要从Spark配置文件中读取相应的配置来访问Spark。
保证系统环境中已经安装好了Java，并设置好了JAVA_HOME，需要使用java来运行spark。

用户通过 :doc:`公有云 <bigflow_on_cloud>` 可以更方便的是使用Bigflow

运行 `bigflow/bin/pyrun` 即可在Python交互式环境中使用Bigflow Python。

接下来，我们可以导入相关的module了::

  from bigflow import base, transforms, input, output

Bigflow Python任务通过 `pipeline` 进行定义，它通过一些列对抽象数据集的变换用户计算任务。同时，pipeline也代表任务最终被提交到的计算引擎上。我们可以首先创建一个本地引擎来快速地试用::

  >>> pipeline = base.Pipeline.create("LOCAL")

接下来可以读一个文件::

  >>> text = pipeline.read(input.TextFile("some input path"))
  >>> print text
  [...]

`pipeline.read()` 将本地文件映射为一个 `P类型` 的实例，更确切的说，是一个 `PCollection` 。PCollection可以看作是一个分布式的Python `list` 结构。TextFile表示读取的类型为文本文件，它会将文本的每一行作为PCollection的元素。注意，这时我们还没有真正地读取数据，仅仅记录了一个文本文件到PCollection的映射，因此 `print text` 显示的内容为省略号("`[...]`")。我们可以使用 `get()` 方法触发Pipeline计算，将PCollection转化为一个Python的内置list::

  >>> print text.get()
  >>> ['An apple', 'a banana', ...(文本文件的每一行内容)]

PCollection定义了一系列的变换方法方便我们对其表示的数据进行计算，例如，可以使用 `count()` 方法统计元素的个数，也就是输入文本的行数::

  >>> line_num = text.count()
  >>> print line_num
  o

记住P类型实例的变换结果是另一个P类型实例，这次我们看到line_num通过一个 `o` 表示它是另一种P类型，PObject，它对应于单个值。我们可以再次调用 `get()` 方法看到结果::

  >>> print line_num.get()
  54

我们可以再使用一个 `distinct()` 方法得到text中所有的不重复元素::

  >>> distinct_lines = text.distinct()
  >>> print distinct_lines
  >>> [...]

`distinct()` 方法的返回结果是一个PCollection，我们仍然可以使用 `get()` 方法得到其内容，或是使用 `Pipeline.write()` 方法把结果写到文件中::

  >>> pipeline.write(distinct_lines, output.TextFile("some output path"))

与 `read()` 方法类似，调用 `write()` 并不会马上触发文件的写操作，而仅仅将这个过程记录下来。这次我们可以使用 `Pipeline.run()` 触发Pipeline计算::

  >>> pipeline.run()

这时，distinct_lines的内容真正被写到了我们指定的输出路径中。

回顾一下，Bigflow Python会仅可能地推迟Pipeline的计算来对整个计算过程进行优化。触发计算的方式只有两种： `get()` 和 `run()` 方法。

更多的变换操作
##############

Bigflow Python提供了众多的变换支持从简单到复杂的计算逻辑。比如说现在我们希望得到之前的文本文件中，单词最多的那一行::

  >>> line_with_num = text.map(lambda line: (len(line.split()), line))
  >>> line_with_num.reduce(lambda a, b: a if (a[0] > b[0]) else b).get()
  (3, "Whatever it uses")

第一行代码使用了 `map()` 变换，将每一行映射为一个tuple -- (单词数, 行内容)，映射结果为另一个PCollection；第二行代码使用 `reduce()` 变换，将PCollection中的每两个元素进行比较，返回单词数更大的元素，最终将所有的元素聚合为一，返回一个PObject。这里的 `map()` 和 `reduce()` 用法非常类似于Python内置的 `map() <https://docs.python.org/2/library/functions.html#map>`_ 和 `reduce() <https://docs.python.org/2/library/functions.html#reduce>`_ 。 变换的参数是一个方法，例子中是Python的 `匿名表达式(lambdas) <https://docs.python.org/2/reference/expressions.html#lambda>`_ 。我们也可以显式定义方法并使用::

  >>> def max(a, b):
  ...    if a[0] > b[0]:
  ...        return a
  ...    else:
  ...        return b
  ...

  >>> line_with_num.reduce(max).get()
  (3, "Whatever it uses")

在当前的分布式计算领域，广为人知的范式便是MapReduce。在Bigflow Python中，用户能够轻易地实现一个MapReduce范式，比如以经典的Word Count为例::

  >>> words = text.flat_map(lambda line: line.split())  # [...]
  >>> groups = words.group_by(lambda word: word, lambda word: 1)  # {k0: [...]}
  >>> result = groups.apply_values(transforms.sum)  # {k0: o}
  >>> print result.get()
  {"Whatever": 1, "it": 3, "use": 2}

第一行代码将输入的每一行映射为多个单词( `flat_map()` 变换是一个1到N的映射)，第二行使用 `group_by()` 变换根据单词进行分组。分组的结果是一种新的P类型 -- PTable。简单而言，PTable可以看作是分布式的Python `dict` 类型，其具有key到另一个P类型的映射::

  >>> print groups
  {k0: [...]}

例子中，groups是一个以单词为key，PCollection为value的PTable，PCollection的包含着多个'1'。我们可以使用 `apply_values()` 方法应用任何的变换到value，也就是PCollection上。例如，之前用过的 `transforms.count()` ::

  >>> result = groups.apply_values(transforms.count)
  >>> print result
  {k0: o}

现在结果是另一个PTable，value变为了PObject(单词数量)。

PTable可以通过 `flatten()` 变换转换为一个PCollection::

  >>> flatten_result = result.flatten()
  >>> print flatten_result
  [...]

PCollection的元素为(key, value) tuple::

  >>> print flatten.get()
  [("Whatever", 1), ("it", 3), ("use", 2)]

Bigflow Python中所有的变换可以在 `transforms` 查看接口说明和用法。

完整的程序
^^^^^^^^^^

批处理引擎（spark）
###################

之前的例子中，Pipeline运行在本地引擎上，生产环境中我们可以使用spark引擎处理真正的大规模数据。

把所有的代码放到一个py文件中，在创建Pipeline的时候指定"spark"作为Pipeline类型::

  """word_cnt.py"""
  from bigflow import base, transforms, input, output

  def word_cnt(p):
      return p.group_by(lambda x: x, lambda x: 1) \
              .apply_values(transforms.sum) \
              .flatten()

  pipeline = base.Pipeline.create("spark", tmp_data_path="some hdfs path")  # now the job runs on DCE
  input_data = pipeline.read(input.TextFile("hdfs:///some input path"))
  result = input_data.flat_map(lambda line: line.split()) \
                     .apply(word_cnt)
  pipeline.write(result, output.TextFile("hdfs:///some output path"))
  pipeline.run()

运行"bin/pyrun word_cnt.py"便可以把任务提交到spark上。

更多使用示例
^^^^^^^^^^
`examples <https://github.com/baidu/bigflow/tree/master/bigflow_python/python/bigflow/example>`_

进阶教程
^^^^^^^^

 * 更多的概念和介绍，请参照Bigflow Python :doc:`编程指南 <guide>`
 * Bigflow Python :doc:`API索引 <rst/modules>` 参考所有API的说明。
