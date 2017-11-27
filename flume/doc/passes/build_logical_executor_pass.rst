=========================
BuildLogicalExecutorPass
=========================

1. 功能介绍
-----------------
BuildLogicalExecutorPass的作用是为LOGICAL节点创建Executor, LOGICAL的条家年如下：

* unit是叶子节点
* unit的父节点不是LOGICAL节点
* unit不包含GlobalPartialMerged标记, 具体请参考 `MergePromoteGlobalPartialPass <merge_promote_global_partial_pass.html>`_
* unit的类型必须是LOAD, SINK, UNION, PROCESS中的一种
* unit没有ExecuteByFather标记, 具体请参考 `BuildMapInputPass <build_map_input_pass.html>`_


2. 依赖说明
-----------
**PASS**

* `BuildPartialExecutorPass <build_partial_executor_pass.html>`_

**ANALYSIS**

* `ScopeLevelAnalysis <../analysises/scope_level_analysis.html>`_

3. 图示说明
-------------
**plan图（运行前）**

无

**plan图（运行后）**

无


`返回 <../plan_pass.html#pass>`_
