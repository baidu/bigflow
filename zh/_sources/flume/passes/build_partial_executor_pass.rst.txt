=========================
BuildPartialExecutorPass
=========================

1. 功能介绍
-----------------
BuildPartialExecutorPass的作用是为PROCESS节点创建Executor, PROCESS节点条件如下：

* unit必须是PROCESS节点
* unit必须具有GlobalPartialMerged标记, 具体请参考 `MergePromoteGlobalPartialPass <merge_promote_global_partial_pass.html>`_
* unit的父节点不能是PARTIAL_PROCESSOR节点。

2. 依赖说明
-----------
**PASS**

无

**ANALYSIS**

无

3. 图示说明
-------------
**plan图（运行前）**

无

**plan图（运行后）**

无


`返回 <../plan_pass.html#pass>`_
