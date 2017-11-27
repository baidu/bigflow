编程指南
========

* `概述`_
* `后端引擎`_
* `配置`_

  * `Hadoop配置`_
  * `bigflow-env.sh`_

* `P类型(PType)`_

  * `PCollection`_
  * `PObject`_
  * `PTable`_
  * `变换`_
  * `apply/apply_values`_

* `输入/输出`_
* `SideInputs`_

  * `闭包`_

* `附带文件`_
* `缓存机制`_
* `PCollection的顺序`_
* `错误信息和日志`_
* `其它`_

概述
====

Bigflow Python是一个致力于简化分布式计算任务编写和维护的Python module，它提供了对分布式数据和计算的高层抽象，你可以使用这些抽象来编写分布式计算程序。Bigflow Python能够将这些抽象映射到不同的分布式计算框架之上。

Bigflow Python中，最重要的抽象被称为 `P类型` ，P类型是分布式数据的抽象描述，非常类似于Spark中的RDD。实际上，它们都借鉴于于Google的论文<<Flume Java>>。P类型可以通过读取(read)接口得到，例如读取文件系统中的文件/目录或是数据库中的表格，或是通过Python的已有变量构造得到。P类型可以通过 `变换` 来描述计算，例如，映射、分组、连接等。最终，变换后的P类型可以通过写出(write)接口持久化到文件系统/数据库中，或是将其转换为内存中的Python内置变量。此外，用户还可以将一个P类型进行缓存以便于多轮迭代计算。

第二个抽象概念为 `SideInputs` ，它指被广播到计算中去的P类型或是Python变量。通常而言，一个计算将被并行地在计算集群中执行，这时每个计算切片都能够得到SideInputs来满足计算的需求(例如查字典)。

后端引擎
========

目前，Bigflow Python支持两种后端执行引擎:

 * Local -- 一个轻量的本地执行引擎，便于学习以及基于小数据集的算法验证。
 * Spark -- 开源分布式计算引擎。

后端执行引擎可以在创建Pipeline的时候指定，例如::

  local_pipeline = base.Pipeline.create("local")
  spark_pipeline = base.Pipeline.create("spark")


配置
====

Hadoop配置
----------

Bigflow Python需要读取core-site.xml配置文件来访问HDFS。因此在使用时，用户需要首先设置好HADOOP_HOME环境变量。

bigflow-env.sh
---------------

bigflow-env.sh位于BIGFLOW_PYTHON_HOME/bigflow/bin下，其中包含一些Bigflow Python在运行时的可选参数。

* Bigflow Python默认将log打印到屏幕，用户可以通过改变下面的参数来指定Log的输出文件::

   BIGFLOW_LOG_FILE="a local directory"

* 对于一个P类型PValue，默认的str(PValue)仅会得到一个表示其具体类别的字符串。也即`print PValue`不会真正地触发计算。这时，如果希望看到一个P类型的内容，可以调用 `PValue.get()` ::

  >>> p1 = pipeline.read(input.TextFile("xxx"))
  >>> print p1
  >>> [...]
  >>> print p1.get()
  >>> ["line1", "line2", ...]

  用户可以设置下面的变量来改变`str(PValue)`的行为 ::

    BIGFLOW_PTYPE_GET_ON_STR=true

  这时，每次`print PValue`都会隐式地执行`PValue.get()`::

  >>> print p1
  >>> ["line1", "line2", ...]

  这样做的好处在于可以使Bigflow Python写出的代码更像一个单机程序。

P类型(PType)
============

P类型是Bigflow设计中的核心抽象，其表示集群中的分布式数据集，P类型的另一个特点是数据不可变，对P类型的任何计算都产生一个新的P类型。

在Bigflow Python中，共有3中不同的P类型，其中的每一种都被设计于类似Python语言的内置类型:

1. :mod:`PCollection <bigflow.pcollection>`  --  并行化的Python `list`
2. :mod:`PObject <bigflow.pobject>`      --  并行化的Python单个变量
3. :mod:`PTable <bigflow.ptable>`       --  并行化的 Python `dict`

PCollection
-----------

PCollection表示并行化的Python `list` 。其可以通过一个Python list实例转换得到，或是读取文件得到::

  >>> p1 = pipeline.read(input.TextFile("xxx"))
  >>> print p1
  [...]
  >>> p2 = pipeline.parallelize([1, 2, 3])
  >>> print p2
  [...]

字符串 "*[...]*" 表示p1/p2均为PCollection。绝大多数的Bigflow变换均作用于PCollection上，而且结果多为PCollection。

PCollection非常类似于Apache Spark中的弹性分布式数据集(RDD)。

PObject
-------

PObject表示单个变量。其往往是聚合类变换的结果，例如max/sum/count等。

  >>> print p1.count()
  o
  >>> print p2.max()
  o

字符"*o*" 表示一个PObject。PObject往往作为一个 `SideInput` 参与到变换中。具体的例子请参照 `SideInput` 部分。

PTable
------

PTable非常类似于并行化的Python `dict`, 其包含key到value的映射，但其value必须是另一个P类型。PTable往往是一个分组变换的结果::

  >>> p3 = pipeline.parallelize([("A", 1), ("A", 2), ("B", 1)]
  >>> p4 = p3.group_by_key()  # group_by_key() 的输入PCollection的所有元素必须是有两个元素的tuple或list。第一个元素为key，第二个元素为value。
  >>> print p4
  {k0: [...]}
  >>> print p4.get()
  {"A": [1, 2], "B": [1]}

字符串 `{k0: [...]}` 表示 `p4` 是一个PTable。 `p4` 的key类型为Python str，value为一个PCollection。

  >>> p5 = p4.apply_values(transforms.max)  # 将变换 `transforms.max` 作用到p4的value，也即PCollection上。
  {k0: o}

`p5` 也是一个PTable，它的key与 `p4` 一致，value变为了PObject类型( `transforms.max` 的结果)。

由于PTable仍然是一个P类型，因此它可以作为另一个PTable的value。例如::

  >>> p6 = pipeline.parallelize({"A": {"a": 1, "e": 2}, "B": {"b": 3, "f": 4}})
  >>> print p6
  {k0: {k1: o}}  # 两层key，最内层value为PObject

  >>> p7 = pipeline.parallelize({"A": {"a", [1, 2]}, "B": {"b": [3, 4]}})
  >>> print p7
  {k0: {k1: [...]}}  # 两层key，最内层value为PCollection

也即，PTable可以无限嵌套，

P类型可能通过下面的三种情况构造:

 * :func:`Pipeline.read() <bigflow.pipeline.pipeline_base.PipelineBase.read>`: 从外部存储读取
 * :func:`Pipeline.parallelize() <bigflow.pipeline.pipeline_base.PipelineBase.parallelize>`: 通过内存变量构造
 * 从另一个P类型 `变换`_ 得到

P类型可以通过下面的三种情况使用:

 * :func:`Pipeline.write() <bigflow.pipeline.pipeline_base.PipelineBase.write>`: 持久化到外部存储
 * :func:`Pipeline.get() <bigflow.pipeline.pipeline_base.PipelineBase.get>`: 转换为Python内置变量
 * 作用一个 `变换`_ ，生成另一个P类型

P类型的不断变换构造出一个有向无环图，最终完整地表达出用户的计算逻辑。

变换
----

Bigflow Python提供了一系列的变换原语方便用户表达计算。

.. autosummary::

  bigflow.transforms.accumulate
  bigflow.transforms.aggregate
  bigflow.transforms.cartesian
  bigflow.transforms.cogroup
  bigflow.transforms.combine
  bigflow.transforms.distinct
  bigflow.transforms.diff
  bigflow.transforms.extract_keys
  bigflow.transforms.extract_values
  bigflow.transforms.filter
  bigflow.transforms.first
  bigflow.transforms.flatten
  bigflow.transforms.flatten_values
  bigflow.transforms.group_by
  bigflow.transforms.group_by_key
  bigflow.transforms.is_empty
  bigflow.transforms.intersection
  bigflow.transforms.join
  bigflow.transforms.left_join
  bigflow.transforms.right_join
  bigflow.transforms.full_join
  bigflow.transforms.max
  bigflow.transforms.max_elements
  bigflow.transforms.min
  bigflow.transforms.map
  bigflow.transforms.flat_map
  bigflow.transforms.reduce
  bigflow.transforms.sort
  bigflow.transforms.sum
  bigflow.transforms.take
  bigflow.transforms.transform
  bigflow.transforms.union
  bigflow.transforms.to_list_pobject
  bigflow.transforms.pipe

以上的所有变换均需要作用于P类型之上，产生另一个P类型。例如，将 `transforms.map` 作用于PCollection上产生一个新的PCollection。如果把PCollection看作Python内置的 `list` ， `transforms.map` 的用法非常类似于Python内置的 `map()` 方法::

  python_list = [1, 2, 3]  # Python list
  pcollection = pipeline.parallelize([1, 2, 3])  # 通过Python list构造出一个PCollection

  map(lambda x: x + 1, python_list)  # 结果: [2, 3, 4]
  transforms.map(pcollection, lambda x: x + 1).get()  # 结果: [2, 3, 4]

PCollection同样有一个成员方法 `PCollection.map(function)`::

  pcollection.map(lambda x: x + 1).get()  # 结果: [2, 3, 4]

实际上， `PCollection.map(function)` 内部的实现就是 `transforms.map(self)` 。

此外，之前的例子也可以这样写::

  pcollection.apply(transforms.map, lambda x: x + 1)

关于 `apply()` 方法，请见下节。

apply/apply_values
------------------

所有的P类型均定义了一个成员方法 `apply()` ，其定义非常简单::

  p.apply(transform, *args)

等价于::

  transform(p, *args)

用户可以根据自己的风格喜好编写代码::

  p.flat_map(...)
   .map(...)
   .reduce(...)

或者这样写::

  p.apply(transforms.flat_map, ...)
   .apply(transforms.map, ...)
   .apply(transforms.reduce, ...)

使用 `apply()` 的一个优点在于，用于可以将多个变换放到一个自定义的方法中，apply这个方法::

  def count_youth_number(ages):
  # 假定age是一个PCollection
      return ages.filter(lambda x: x <= 18).count()

  ages.apply(count_youth_number)

可以看到，apply自定义方法保持了风格的统一，同时使得代码更具有内聚性，更容易复用。

类似于 `apply()` ，PTable的成员方法 `apply_values()` 能够将一个变换作用在它的value上::

  >>> pt = pipeline.parallelize({"A": [("a", 1), ("a", 2)], "B": [("b", 3), ("b", 4)]})
  >>> print pt
  {k0: [...]}  # pt的value是一个PCollection
  >>> po = pt.apply_values(transforms.count)  # transforms.count(PCollection) --> PObject
  >>> print po
  {k0: o}  # po的value是一个PObject

value中的PCollection还可以用group_by_key进行再分组::

  >>> pt2 = pt.apply_values(transforms.group_by_key)
  >>> print pt2
  {k0: {k1: [...]}}  # pt2有两层key(经过了两次分组)
  >>> print pt2.get()
  {"A": {"a": [1, 2]}, "B": {"b": [3, 4]}}

可以看到，最终结果是一个嵌套的PTable。

PTable可以通过 `flatten()` 方法转换("打平")为一个PCollection: 每个key和其对应的value中所有元素将构成一个(k, v)的Python tuple::

  >>> pc = pt.flatten()
  >>> print pc
  [...]
  >>> print pc.get()
  [("B", ("b", 3)), ("B", ("b", 4)), ("A", ("a", 1)), ("A", ("a", 2))]

对于多层嵌套的PTable， `flatten()` 将把所有的key均打平，最终结果还是一个PCollection::

  >>> print pt2.flatten().get()
  [("B", ("b", 3)), ("B", ("b", 4)), ("A", ("a", 1)), ("A", ("a", 2))]

这种情况下，如果希望仅打平value中的PTable，可以使用apply_values，即 `apply_values(transforms.flatten)`::

  >>> print pt2.apply_values(transforms.flatten).get()
  {"A": [("a", 1), ("a", 2)], "B": [("b", 3), ("b", 4)]}

输入/输出
=========

Bigflow Python提供了Pipeline.read()方法从外部存储读取数据，以及Pipeline.write()方法将数据写出。输入/输出类型被抽象为 `input.Source` 和 `output.Target` ，例如，input.TextFile是一个对应于文本文件的Source。

目前，Pipeline.read()/write()的结果只能为PCollection。所有已实现的Source/Target如下:

  ====================================   ==============================================================
  Source                                 对应的PCollection元素
  ====================================   ==============================================================
  :class:`bigflow.input.TextFile`        文本文件中的每一行(\n分割)
  :class:`bigflow.input.SchemaTextFile`  文本文件中的每一行(\n分割), 用户可指定字段分割符(默认是\t)，生成支持字段操作的PCollection
  :class:`bigflow.input.SequenceFile`    Key/Value均为bytes，由用户自定义serde解析
  ====================================   ==============================================================

  ====================================   ==============================================================
  Target                                 输出内容
  ====================================   ==============================================================
  :class:`bigflow.output.TextFile`       每个元素为一行，内容为PCollection中每个元素调用 `str()` 后的字符串结果
  :class:`bigflow.output.SchemaTextFile` 每个元素为一行，内容为支持字段操作PCollection每个元素按照用户定义的字段顺序输出的结果, 且可指定字段输出分割符(默认是\t)
  :class:`bigflow.output.SequenceFile`   由用户自定义serde指定Key/Value如何被写为Bytes
  ====================================   ==============================================================

注意，这里的SequenceFile实际上为Hadoop中的SequenceFileAsBinaryInputFormat/SequenceFileAsBinaryOutputFormat。也就是说，Key/Value均为BytesWritable。

SideInputs
==========

当Bigflow Python提交任务时，计算被并行执行。但某些情况下，用户会期望将某些PType传入到另一个PType的变换中参与计算(由于变换实际在运行时被分布地计算，因此更确切地说，是被 *广播* 到了变换)。例如，如果希望在Bigflow中进行 *mapper-side join* ，一个较小的PCollection可以以字典的方式传入到较大PCollection/PTable的 `flat_map()` 变换里面。当框架遍历较大PCollection/PTable每个元素时，可以直接对较小PCollection进行遍历来查找满足需要的连接条件。

大部分的分布式计算引擎本身提供了一些机制来满足这样的场景，例如Hadoop的 `DistributeCache` 或是Spark中的 `Broadcast varirables` 。

在Bigflow中，这样的机制被抽象为 `SideInputs` :你可以将一个P类型作为参数传入到另一个P类型的变换中。

绝大多数的变换均支持SideInputs，例如 :func:`map() <bigflow.transforms.map()>`, :func:`flat_map() <bigflow.transforms.flat_map>`, :func:`filter() <bigflow.transforms.filter>` 等::

  >>> p1 = pipeline.parallelize([3, 7, 1])
  >>> p2 = pipeline.parallelize(4)

  >>> result = p1.filter(lambda x, threshold: x < threshold , p2)  # 注意这里的lambda表达式：`threshold` 为 `p2` 在运行时的值
  >>> print result.get(result)
  [3, 1]

上面的例子中，p2是一个包含单个元素 `4` 的PObject。它被作为SideInputs传入到 `filter()` 变换中，因此对应的变换表达式从原来的一个输入x，变成了有两个输入: `x` 和 `threshold` ， `threshold` 为PObject在运行时的值，也就是4。

闭包
----

SideInputs的用法有些类似于 `闭包 <http://en.wikipedia.org/wiki/Closure_(computer_programming)>`_ 。当然，闭包在Bigflow Python中也同样支持::

  >>> threshold = ...  # 假定我们经过某些计算，得到了一个Python int，值为4
  >>> print threshold
  4
  >>> result = p1.filter(lambda x: x < threshold)
  [3, 1]

这时候，我们没有使用SideInputs: `threshold` 是一个内存变量，它被filter中的lambda方法所捕获，同时也能够被正确地处理。

附带文件
========

通常而言，当使用Bigflow Python编写的代码被提交到分布式环境中运行时，相关的方法能够自动地被序列化/反序列化到集群中。然而用户的代码可能需要从本机import一些自己定义的库。此外，用户还可能需要将一些资源文件一起随代码进行提交，并在运行时读取资源。

对于这样的需求，可以使用 `Pipeline.add_file()` 方法::

  >>> import user_defined_module
  >>> pipeline.add_file("PATH_OF_user_defined_module", 'REL_PATH_ON_CLUSTER')
  >>> c = pipeline.read(...)
  >>> d = c.map(user_defined_module.some_function)
  >>> d.get()

如果需要添加的文件较多，可以使用 `Pipeline.add_directory()` 方法，将一个目录下的所有文件一起打包::

  >>> from aa.bb import cc, dd, ...
  >>> pipeline.add_directory("PATH_OF_a_module", "REL_PATH_ON_CLUSTER")
  >>> ...

缓存机制
========

Pipeline的运行有两种机制触发:

 * 调用 `PType.get()` 或者 `Pipeline.get(PType)` ，两者等价。
 * 调用 `Pipeline.run()`

当Pipeline通过 `PType.get()` 触发运行时，P类型中的数据实际上首先被缓存起来，然后读取到内存中。因此如果一个P类型被调用多次 `PType.get()` ，从第二次开始，Pipeline并不会真正地做计算，而是直接读取缓存数据。除了 `get()` 方法之外，显示地调用 `PType.cache()` 也可以要求Bigflow Python在运行时将P类型数据缓存，并在之后的计算中直接读取缓存数据。缓存机制在需要多轮迭代计算时会很有用，例如实现PageRank算法。::

  >>> lines = pipeline.read(input.TextFile("hdfs:///lines.txt")
  >>> len = lines.count()
  >>> lines.cache()
  >>> while len.get() > 100:
  ...     lines = lines.flat_map(some_transforms)
  ...     lines.cache()
  ...     len = lines.count()
  ...
  >>> pipeline.write(lines, .output.TextFile("some output path"))

PCollection的顺序
=================

对于分布式计算而言，元素的顺序往往难以保证。通常而言，PCollection应当被认为是无序数据集，只有 `sort()` 方法的 **直接结果** 保证元素的顺序::

  p = pipeline.parallelize([...])
  sorted = p.sort()  # sorted中元素降序排列

如上文所言，任何基于sorted的后续变换均不再保证顺序，例如::

  mapped = sorted.map(lambda x: x)

此时不应当再假定mapped中元素仍然有序。
