=============================
BuildTaskExecutorPass
=============================

1. 功能介绍
-----------------
BuildTaskExecutorPass的作用是将所有TASK节点以下, 叶结点以上的中间节点所具有的Executor信息分别汇集
到各自的父节点当中。

2. 依赖说明
-----------
**PASS**

* `BuildAggregatorPass <build_aggregator_pass.html>`_
* `BuildDummyExecutorPass <build_dummy_executor_pass.html>`_
* `BuildPartialExecutorPass <build_partial_executor_pass.html>`_
* `BuildLogicalExecutorPass <build_logical_executor_pass.html>`_
* `BuildLocalShuffleExecutorPass <build_local_shuffle_executor_pass.html>`_
* `BuildPartialShuffleExecutorPass <build_partial_shuffle_executor_pass.html>`_
* `BuildMergeShuffleExecutorPass <build_merge_shuffle_executor_pass.html>`_

**ANALYSIS**

* `ExecutorDependencyAnalysis <../analysises/executor_dependency_analysis.html>`_
* `ScopeLevelAnalysis <../analysises/scope_level_analysis.html>`_

3. 图示说明
-------------
**plan图（运行前）**

无

**plan图（运行后）**

无


`返回 <../plan_pass.html#pass>`_
