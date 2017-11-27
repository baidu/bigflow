=========================
BuildDummyExecutorPass
=========================

1. 功能介绍
-----------------
BuildDummyExecutorPass的作用是将所有被标记成DUMMY的节点修改成为DUMMY节点, 并且为其创建Executor。
关于DUMMY节点的创建, 具体请参考

* `PromoteUnionDownstreamPass <promote_union_downstream_pass.html>`_
* `PromoteGlobalParitalNodePass <promote_global_partial_node_pass.html>`_

2. 依赖说明
-----------
**PASS**

* `BuildShuffleWriterReaderPass <build_shuffle_writer_reader_pass.html>`_

**ANALYSIS**

无

3. 图示说明
-------------
**plan图（运行前）**

无

**plan图（运行后）**

无


`返回 <../plan_pass.html#pass>`_
