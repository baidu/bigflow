################
传输数据编码格式
################

本文档描述了一种编码算法, 可以把多路/多种分组方式的多条数据流编码成一条Key/Value数据流.
该算法用于跨Task的数据传输和归并.


背景
====

关于计算和数据模型, 请先参见 :doc:`core`. 在逻辑执行计划中, 根据算法的需求, 我们有很多路数据,
每路数据都有不同的分组方式. 然而在任意一个特定的分布式框架, 都只支持有限种分组方式,
在数据的路数上往往也有限制.

所以, 我们需要一种编码方式, 能够把以任意方式分组的多路数据合并成一路,
使得这路数据流只要按照唯一的Key进行合并/排序, 就可以高效的进行解码并进行处理. 具体来说,
就是满足下述两个要求:

1. 得到一个Key, 就能从中解码出该条数据属于哪个Logical Plan Node, 以及其所属Scope的Key.


2. 编码后的Key按照逐字节比较的方式, 就能得到需要的shuffle顺序, 不需要自定义比较函数.

   a. 对于同一个Scope下的不同分组, 可以按照相应key的顺序进行传输.
      不同分组的数据不能重叠在合并后的流中, 总是把前一组数据传输完毕, 才会继续传输下一组.


   b. 对于同一个Scope下不同路的数据, 可以指定每路到达的顺序, 即让某些路比其它路先到达.


   这样可以尽量降低需要进行缓存的数据量.


编码格式
========

首先给出整体的编码方式
::

    shuffle-key ::= {[scope-tag] scope-key } [node-tag] {order-key}

    scope-tag ::= fix-size-integer

    scoped-key ::= fix-size-encoding | escape-encoding | last-element-encoding

    node-tag ::= fix-size-integer

    order-key ::= escape-encoding

    fix-size-integer ::= uint8 | uint16 | uint32 | uint64


下面具体解释一下各个字段的意义.

1. scope-tag/node-tag.

   标记该条数据属于哪个Scope/Node, 整数(uint8/uint16/uint32/uint64).
   整数位数由参与编码的路数决定, 如果某个Scope下只有一路子Scope/Node参与编码,
   则相应的scope-tag/node-tag被省略. 这些tag也能起到调整shuffle顺序的作用,
   tag排在前面那路会被优先传输.


2. scope-key/order-key

   如果某个Node上面有N个Scope, 则这个Node中的每条Record都会附带有N个Key.
   在通过分布式框架进行传输/归并的过程中, 一些Scope需要在传输后恢复相应的Scope/Key,
   比如做GroupBy的时候. 在另一些时候, 只需要保持顺序即可, 不必恢复出具体的Key出来,
   例如做SortBy的时候.


3. fix-size-encoding.

   适用于对固定长度的Key进行编码. 编码时只存储数据内容, 不存储长度.
   当ShuffleNode的type为BySequence时, 得到的partition就可以采用这种方式进行编码.


4. escape-encoding.

   适用于对变长字符串进行编码. 对字符串中的'\\0''\\1'两种字符用'\\1'进行转义,
   即编码为'\\1\\0'和'\\1\\1', 尾部以'\\0'结束.
   这种编码的优点在于在多个Key的情况下能够保证比较顺序. 比如考虑两组数据
   ::

       r0 = ("ab", '\0')
       r1 = ("a", 'c')
       assert(r0 > r1)
   如果直接串联两个字段(字符串和整数), 因为第二个字段为固定长度的整数, 也能够正常解码.
   这时编码后的结果如下
   ::

       simple_encode_0 = "ab\0"
       simple_encode_1 = "ac"
       assert(simple_encode_0 < simple_encode_1)
   这时如果把编码后的字符串进行逐字节比较, 得到结果和直接比较的结果相反.
   而转义编码则不存在这个问题
   ::

       escape_encode_0 = "ab\0\0"
       escape_encode_1 = "a\0b"
       assert(escape_encode_0 > escape_encode_1)


5. last-element-encoding.

   适用于对最后一个Key进行编码. 这时我们不需要存储Key的长度, 只需一直读到buffer的尾部即可完成解码.



使用场景&实例分析
=================

1. 单路Groupby

   对于常见的单路Groupby, 只有一个Scope和一个Node参与Groupby, 这时不需要scope-tag和node-tag.
   因为只有一个Key, 所以可以采用last-element-encoding. 所以最终编码的结果就是原始Key.


2. 两路Join

   两路Join典型的逻辑计划是单个Scope, 其中有两个Node. 这时按照规则得到的编码格式是
   (escaped-key) + (0|1). 在shuffle后, 对于每一组数据, 第0路会先到达并缓存, 第1路后到达并流式处理,
   即可完成迪卡尔积的计算. 在这个过程中, 只有第0路需要被缓存起来.


3. 单路GroupBy + Distinct

   该场景下典型的逻辑计划为一组嵌套的Scope下面的一个Node. 因为每一层只有一个Scope/Node,
   tag可以被省略. GroupBy Key需要转义, Distinct Key因为在最末尾, 不需要被转义. 这样Key编码就是
   escaped-group-key + distinct-key


4. 单Task同时处理多个维度的分组

   这种场景会在Task合并优化后出现, 起到平衡Task负载的优化效果.
   我们假设这里把多个GroupBy逻辑合并到一个Task中处理, 这时的编码就是scope-tag + scope-key.
   因为每个Scope内部只有单一的Node, 故不需要node-tag. 因为scope-key在最尾部, 故不需要转义.


现有编码方式及改进
==================

DCE下目前采用的编码方式为
::

    reduce-key ::= priority-tag partition begin-sep { scope-key } end-sep task-id tag

    priority-tag ::= uint8

    partition ::= uint32

    begin-seq ::= "\0\0"

    scope-key ::= escape-encoding '\1'

    end-seq ::= "\0\0"

    task-id ::= uint16

    tag ::= uint16

目前的编码方式主要用来提供两个保证:

1. 可以区分出来自不同Task/Partition的数据,

   因为DCE不支持多输出, 所以在特定的拓扑下一个Task有可能接受到本应由其它Task处理的数据.
   另外, 把Partition信息编码到Key中, 也方便实现HCE要求的Partitioner. 前置的partition,
   后面的task-id即是为此而设置.


2. 嵌套Scope中来自于父Scope的数据先传输.

   该保证是为了支持混合方式Shuffle, 即某些属于先传输到Reduce端进行Local Shuffle,
   再和MR传输的数据进行合并. priority-tag和begin-seq/end-seq即是为了这个目的设置.


目前的编码方式的缺点在于对于原始的Key增加了过多的额外编码. 对于单Task/单Partition的情形,
单路Groupby的场景, 本来只存储原始Key就能解决问题, 但是目前的编码方式至少多用了14个字节. 实际上,
至少在我们的benchmark中, 大部分场景下的原始Key不超过10个字节.

结合上文提到过的编码方式, 我们可以采用如下方式进行编码:
::

    reduce-key ::= task-partition-encoding shuffle-key
    task_partition-encoding ::= task-bits partition-bits seq-bit

1. 根据全局的task数量及partitioin数量, 决定修要几个bit用来存储Task-Id, 几个bit用来存储partition.
   目前来说, 大部分任务可以用uint16来满足. 这里需要注意的是,
   因为考虑到对DCE合并partition功能的支持, 我们预留一个bit, 用来作为partition间的分隔符,
   以区分空数据的task.


2. Task内部的数据采用上文的编码方式进行编码. 这个编码方式对于不同场景的适应性比较好,
   不会出现目前编码方式中对简单case浪费大量编码空间的问题.


