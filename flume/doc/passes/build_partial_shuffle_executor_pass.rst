===============================
BuildPartialShuffleExecutorPass
===============================

1. 功能介绍
-----------------
BuildLogicalExecutorPass的作用是为PARTIAL_SHUFFLE节点创建Executor, 同时更新Executor信息。

2. 依赖说明
-----------
**PASS**

* `BuildLogicalExecutorPass <build_logical_executor_pass.html>`_

**ANALYSIS**

* `PartialNodeAnalysis <../analysises/partial_node_analysis.html>`_

3. 图示说明
-------------
**plan图（运行前）**

无

**plan图（运行后）**

无


`返回 <../plan_pass.html#pass>`_
