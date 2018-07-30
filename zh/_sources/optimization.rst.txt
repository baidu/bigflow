Bigflow优化简介
-------------------

Bigflow的优化层被称作Flume，是百度基础架构部和和大数据部联合的一个项目。

flume中优化工作很多，且许多优化与flume设计中的一个scope的概念关联极大，对于一般用户而言较难理解，
本文不按照flume优化层的概念进行全面分析，而是从用户的角度来阐述一些比较直观的优化工作。

这里通过一些实例简述一些Bigflow翻译到MR平台上时进行的优化工作，以及Bigflow所使用的Hadoop平台的可带来性能提升的高级特性。
(Spark上的优化与此类似，后续补充）

1. 尽可能的把算子合并到同一个Mapper/Reducer中。

    例如：::

        def sum_nums_in_line(lines):
            return  (lines.flat_map(lambda line: line.split())
                .map(lambda x: int(x))
                .map(lambda x: x * 2)
                .filter(lambda x: x > 2)
            )

        lines = pipeline.read(input.TextFile('hdfs:///inputfile'))
        print lines.apply(sum_nums_in_line).get()

    如果使用MR模式，这段代码将只会启动一个Map-Only的Task，并且计算会先读取第一条数据，
    执行整个流程并输出（或过shuffle）后才返回读取并计算第二条数据，
    整个计算过程中不需要把所有的数据缓存到内存中。

2. 使用了Hadoop的DAGMR引擎，如果用户作业有多层级级联的情况，将会有十分明显的优化效果。

    例如，如果用户原来的作业有三个MR Job，我们分别称之为MR1，MR2， MR3，
    其中 MR1的输出和MR2的输出是MR3的输入，则Bigflow翻译出来的作业将会翻译成如下的一个DAGMR作业：

    .. image:: static/dag-example.jpg
        :width: 200px

    其中MR1的Reducer,MR2的Reducer,MR3的Mapper会合并到同一个vertex里（图中的vertex1），这样，
    Reducer1,Reducer2就不再需要把输出写到HDFS，而是直接从内存中交给了MR3的Mapper，
    减少一次数据落盘，当作业级联级数较多时，优化效果十分明显。

3. partial操作前移以减少shuffle数据量。

    例如如下wordcount代码 ::

        print lines.flat_map(lambda line: line.split()) \
            .group_by(lambda word: word) \
            .apply_values(transforms.count) \
            .get()

    count操作会被拆成两部分，第一个操作是一个partial的count算子，第二个是一个non-parital的sum算子。

    其中partial的count算子会被前移到map端，也就是说，会在map端把数据先在内存中分组，
    并在每个分组上统计该分组里有多少个单词，然后到reduce端会把这些count之后的结果sum起来，
    得出最终的结果，从而，使得过shuffle的数据量减少很多。

4. 利用MR-Shuffle排序实现多层级分组，减少内存占用量。

    例如如下代码：::

        def count_distinct(p):
            return p.distinct().count()

        print data.group_by_key() \
                  .apply_values(count_distinct) \
                  .get()


    这段代码完成了先按一个key分组，然后又对value求count_distinct的需求。

    使用目前绝大部分分布式计算引擎，都需要用户自己实现一个单机的处理函数，
    以求出Iterable<V>中有多少个不同的V，以spark为例，代码可能会是这样的：::

        def count_distinct(kvs):
            key = kvs[0]
            vals = kvs[1]
            distinct = set()
            for v in vals:
                distinct.add(v)
            return (key, len(distinct))

        print data.group_by_key() \
                  .map(count_distinct)\
                  .get()

    但这样很容易爆内存，因为spark的group_by_key需要把同key下的所有数据放到内存中。

    于是，对spark比较熟悉的人会提出可以使用combineByKey等接口，
    这样，就可以把distinct的操作前移到map端先计算一遍，
    一降低了爆内存的风险，二减少了shuffle数据量。
    但实际上该方案依然有较大爆内存的风险，
    因为所有distinct的value依然不可避免的需要全部在Reduce内存中存储一份。

    而使用Bigflow，则用户可以直接使用最上面的那段代码，观察该接口可以发现，这段代码
    一来可以复用在全局上进行count_distinct的函数，提升了代码的可重用性；
    二来Bigflow框架能够知道更多计算的细节，count_distinct对于Bigflow而言不是一个黑盒，
    Bigflow实际优化时，会完成以下几件事：

    1. shuffle时，以原key+value为key进行排序，用原key进行partition。

        这样，到reduce端，一是可以根据上下两条数据里key是否相同判断是否是同一个组里的数据，
        二是在每个组内，根据value是否相同即可知道是出现了一个新的value，
        从而可以流式的统计出有多少个不同的元素。
        从根本上杜绝了爆内存的可能。

    2. distinct操作以及相关的分组操作会先在map端进行一次预处理，从而达到减少shuffle数据量的目的。
