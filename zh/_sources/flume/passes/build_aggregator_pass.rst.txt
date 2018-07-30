=========================
BuildAggregatorPass
=========================

1. 功能介绍
-----------------
BuildAggregatorPass的作用是将满足下列条件的LOCAL_SHUFFLE节点转变成为Aggregator节点, 其中条件如下：

* unit为LOCAL_SHUFFLE
* unit的父节点不为TASK节点
* unit本身不要求排序
* unit的孩子节点中至少包含一个PROCESS节点

2. 依赖说明
-----------
**PASS**

* `BuildShuffleWriterReaderPass <build_shuffle_writer_reader_pass.html>`_

**ANALYSIS**

无

3. 图示说明
-------------
**plan图（运行前）**

    .. image:: ../static/passes/build_aggregator_pass_0.png
       :width: 600px

上图中, 计划中包含一个满足条件的LOCAL_SHUFFLE节点。

**plan图（运行后）**

    .. image:: ../static/passes/build_aggregator_pass_1.png
       :width: 600px

对plan应用了BuildAggregatorPass之后, 满足条件的LOCAL_SHUFFLE节点被修改成为AGGREGATE。


`返回 <../plan_pass.html#pass>`_
